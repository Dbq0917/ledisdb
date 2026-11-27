package ledis

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/ledisdb/ledisdb/store"
	"github.com/siddontang/go/snappy"
)

// dumpMagic marks a dump file carrying an environment fingerprint.
// Legacy dumps start directly with the 8 bytes big endian commit id.
var dumpMagic = []byte("LEDUMP01")

// DumpHead is the head of a dump.
type DumpHead struct {
	CommitID uint64

	// fingerprint of the environment the dump was created in,
	// empty for legacy dumps without a fingerprint
	Engine          string
	EncodingVersion uint16
	Databases       uint16
}

// Read reads meta from the Reader.
func (h *DumpHead) Read(r io.Reader) error {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return err
	}

	if !bytes.Equal(magic[:], dumpMagic) {
		// legacy dump, the first 8 bytes are the commit id
		h.CommitID = binary.BigEndian.Uint64(magic[:])
		return nil
	}

	var engineLen uint8
	if err := binary.Read(r, binary.BigEndian, &engineLen); err != nil {
		return err
	}

	engine := make([]byte, engineLen)
	if _, err := io.ReadFull(r, engine); err != nil {
		return err
	}
	h.Engine = string(engine)

	if err := binary.Read(r, binary.BigEndian, &h.EncodingVersion); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &h.Databases); err != nil {
		return err
	}

	return binary.Read(r, binary.BigEndian, &h.CommitID)
}

// Write writes meta to the Writer
func (h *DumpHead) Write(w io.Writer) error {
	if len(h.Engine) > 255 {
		return fmt.Errorf("engine name %q too long for dump head", h.Engine)
	}

	if _, err := w.Write(dumpMagic); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, uint8(len(h.Engine))); err != nil {
		return err
	}

	if _, err := w.Write([]byte(h.Engine)); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, h.EncodingVersion); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, h.Databases); err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, h.CommitID)
}

// checkDumpHead verifies that the dump was created in a compatible
// environment before any data is touched.
func (l *Ledis) checkDumpHead(h *DumpHead) error {
	if len(h.Engine) == 0 {
		// legacy dump without fingerprint, nothing to check
		return nil
	}

	if engine := l.ldb.String(); h.Engine != engine {
		return fmt.Errorf("dump environment mismatch: dump was created with storage engine %q, but current instance uses %q", h.Engine, engine)
	}

	if h.EncodingVersion != keyEncodingVersion {
		return fmt.Errorf("dump environment mismatch: dump uses key encoding version %d, but current instance uses %d", h.EncodingVersion, keyEncodingVersion)
	}

	if databases := uint16(l.cfg.Databases); h.Databases != databases {
		return fmt.Errorf("dump environment mismatch: dump was created with %d databases, but current instance is configured with %d", h.Databases, databases)
	}

	return nil
}

// DumpFile dumps data to the file
func (l *Ledis) DumpFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return l.Dump(f)
}

// Dump dumps data to the Writer.
func (l *Ledis) Dump(w io.Writer) error {
	var err error

	var commitID uint64
	var snap *store.Snapshot

	l.wLock.Lock()

	if l.r != nil {
		if commitID, err = l.r.LastCommitID(); err != nil {
			l.wLock.Unlock()
			return err
		}
	}

	if snap, err = l.ldb.NewSnapshot(); err != nil {
		l.wLock.Unlock()
		return err
	}
	defer snap.Close()

	l.wLock.Unlock()

	wb := bufio.NewWriterSize(w, 4096)

	h := &DumpHead{
		CommitID:        commitID,
		Engine:          l.ldb.String(),
		EncodingVersion: keyEncodingVersion,
		Databases:       uint16(l.cfg.Databases),
	}

	if err = h.Write(wb); err != nil {
		return err
	}

	it := snap.NewIterator()
	defer it.Close()
	it.SeekToFirst()

	compressBuf := make([]byte, 4096)

	var key []byte
	var value []byte
	for ; it.Valid(); it.Next() {
		key = it.RawKey()
		value = it.RawValue()

		if key, err = snappy.Encode(compressBuf, key); err != nil {
			return err
		}

		if err = binary.Write(wb, binary.BigEndian, uint16(len(key))); err != nil {
			return err
		}

		if _, err = wb.Write(key); err != nil {
			return err
		}

		if value, err = snappy.Encode(compressBuf, value); err != nil {
			return err
		}

		if err = binary.Write(wb, binary.BigEndian, uint32(len(value))); err != nil {
			return err
		}

		if _, err = wb.Write(value); err != nil {
			return err
		}
	}

	return wb.Flush()
}

// LoadDumpFile clears all data and loads dump file to db
func (l *Ledis) LoadDumpFile(path string) (*DumpHead, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return l.LoadDump(f)
}

// LoadDump clears all data and loads dump file to db
func (l *Ledis) LoadDump(r io.Reader) (*DumpHead, error) {
	l.wLock.Lock()
	defer l.wLock.Unlock()

	rb := bufio.NewReaderSize(r, 4096)

	h := new(DumpHead)

	var err error
	if err = h.Read(rb); err != nil {
		return nil, err
	}

	if err = l.checkDumpHead(h); err != nil {
		return nil, err
	}

	if err = l.flushAll(); err != nil {
		return nil, err
	}

	var keyLen uint16
	var valueLen uint32

	var keyBuf bytes.Buffer
	var valueBuf bytes.Buffer

	deKeyBuf := make([]byte, 4096)
	deValueBuf := make([]byte, 4096)

	var key, value []byte

	wb := l.ldb.NewWriteBatch()
	defer wb.Close()

	n := 0

	for {
		if err = binary.Read(rb, binary.BigEndian, &keyLen); err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			break
		}

		if _, err = io.CopyN(&keyBuf, rb, int64(keyLen)); err != nil {
			return nil, err
		}

		if key, err = snappy.Decode(deKeyBuf, keyBuf.Bytes()); err != nil {
			return nil, err
		}

		if err = binary.Read(rb, binary.BigEndian, &valueLen); err != nil {
			return nil, err
		}

		if _, err = io.CopyN(&valueBuf, rb, int64(valueLen)); err != nil {
			return nil, err
		}

		if value, err = snappy.Decode(deValueBuf, valueBuf.Bytes()); err != nil {
			return nil, err
		}

		wb.Put(key, value)
		n++
		if n%1024 == 0 {
			if err = wb.Commit(); err != nil {
				return nil, err
			}
		}

		// if err = l.ldb.Put(key, value); err != nil {
		// 	return nil, err
		// }

		keyBuf.Reset()
		valueBuf.Reset()
	}

	if err = wb.Commit(); err != nil {
		return nil, err
	}

	if l.r != nil {
		if err := l.r.UpdateCommitID(h.CommitID); err != nil {
			return nil, err
		}
	}

	return h, nil
}

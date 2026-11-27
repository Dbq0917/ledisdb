package ledis

import (
	"bytes"
	"encoding/binary"
	"os"
	"strings"
	"testing"

	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/store"
)

func TestDump(t *testing.T) {
	cfgM := config.NewConfigDefault()
	cfgM.DataDir = "/tmp/test_ledis_master"

	os.RemoveAll(cfgM.DataDir)

	master, err := Open(cfgM)
	if err != nil {
		t.Fatal(err)
	}

	cfgS := config.NewConfigDefault()
	cfgS.DataDir = "/tmp/test_ledis_slave"
	os.RemoveAll(cfgS.DataDir)

	var slave *Ledis
	if slave, err = Open(cfgS); err != nil {
		t.Fatal(err)
	}

	db, _ := master.Select(0)

	db.Set([]byte("a"), []byte("1"))
	db.Set([]byte("b"), []byte("2"))
	db.Set([]byte("c"), []byte("3"))

	if err := master.DumpFile("/tmp/testdb.dump"); err != nil {
		t.Fatal(err)
	}

	if _, err := slave.LoadDumpFile("/tmp/testdb.dump"); err != nil {
		t.Fatal(err)
	}

	it := master.ldb.RangeLimitIterator(nil, nil, store.RangeClose, 0, -1)
	for ; it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()

		if v, err := slave.ldb.Get(key); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(v, value) {
			t.Fatal("load dump error")
		}
	}

	master.Close()
	slave.Close()
}

func TestDumpEnvMismatch(t *testing.T) {
	cfgM := config.NewConfigDefault()
	cfgM.DataDir = "/tmp/test_ledis_dump_mismatch_master"
	os.RemoveAll(cfgM.DataDir)

	master, err := Open(cfgM)
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()

	db, _ := master.Select(0)
	db.Set([]byte("a"), []byte("1"))

	dumpPath := "/tmp/testdb_mismatch.dump"
	if err := master.DumpFile(dumpPath); err != nil {
		t.Fatal(err)
	}

	cfgS := config.NewConfigDefault()
	cfgS.DataDir = "/tmp/test_ledis_dump_mismatch_slave"
	cfgS.Databases = 8
	os.RemoveAll(cfgS.DataDir)

	slave, err := Open(cfgS)
	if err != nil {
		t.Fatal(err)
	}
	defer slave.Close()

	sdb, _ := slave.Select(0)
	sdb.Set([]byte("exist"), []byte("keep"))

	if _, err := slave.LoadDumpFile(dumpPath); err == nil {
		t.Fatal("load dump must be refused on environment mismatch")
	} else if !strings.Contains(err.Error(), "environment mismatch") {
		t.Fatalf("error must report environment mismatch, got %s", err.Error())
	}

	// the refused load must not touch existing data
	if v, err := sdb.Get([]byte("exist")); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(v, []byte("keep")) {
		t.Fatal("existing data must not be cleared by a refused load")
	}
}

func TestLoadLegacyDump(t *testing.T) {
	cfgM := config.NewConfigDefault()
	cfgM.DataDir = "/tmp/test_ledis_dump_legacy_master"
	os.RemoveAll(cfgM.DataDir)

	master, err := Open(cfgM)
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()

	db, _ := master.Select(0)
	db.Set([]byte("a"), []byte("1"))

	var buf bytes.Buffer
	if err := master.Dump(&buf); err != nil {
		t.Fatal(err)
	}

	// strip the fingerprint and keep a legacy head: commit id + records
	data := buf.Bytes()
	engineLen := int(data[8])
	headLen := 8 + 1 + engineLen + 2 + 2
	legacy := data[headLen:]

	cfgS := config.NewConfigDefault()
	cfgS.DataDir = "/tmp/test_ledis_dump_legacy_slave"
	os.RemoveAll(cfgS.DataDir)

	slave, err := Open(cfgS)
	if err != nil {
		t.Fatal(err)
	}
	defer slave.Close()

	h, err := slave.LoadDump(bytes.NewReader(legacy))
	if err != nil {
		t.Fatal(err)
	}

	if h.CommitID != binary.BigEndian.Uint64(legacy[:8]) {
		t.Fatal("invalid legacy commit id")
	}

	sdb, _ := slave.Select(0)
	if v, err := sdb.Get([]byte("a")); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(v, []byte("1")) {
		t.Fatal("load legacy dump error")
	}
}

package boltTime

import (
	"os"
	"testing"
	"time"
)

func TestPut(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error()
	}
	defer os.Remove("test.db")
	testEntry := Entry{
		Time:  time.Now(),
		Value: []byte("testValue"),
	}

	err = bt.Put(testEntry)
	if err != nil {
		t.Error()
	}
}

func TestGetLatestN(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error()
	}
	defer os.Remove("test.db")

	entries, err := bt.GetLatestN(1)
	if entries != nil {
		t.Error("should be 0")
	}
	if err == nil {
		t.Error(err)
	}

	// add one entry
	testEntry := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put(testEntry)
	if err != nil {
		t.Error(err)
	}

	entries, err = bt.GetLatestN(1)
	if err != nil {
		t.Error(err)
	}
	if len(entries) != 1 {
		t.Error("should only contain 1 entry but contained", len(entries))
		return
	}
	if string(entries[0].Value) != "testValue1" {
		t.Error("mismatch")
	}

	// put multiple
	err = bt.Put(Entry{
		Time:  time.Now().Add(time.Minute),
		Value: []byte("testValue2"),
	})
	if err != nil {
		t.Error()
	}

	err = bt.Put(Entry{
		Time:  time.Now().Add(time.Minute * 2),
		Value: []byte("testValue3"),
	})
	if err != nil {
		t.Error()
	}

	entries, err = bt.GetLatestN(1)
	if err != nil {
		t.Error(err)
	}
	if len(entries) != 1 {
		t.Error("should only contain 1 entry but contained", len(entries))
		return
	}
	if string(entries[0].Value) != "testValue3" {
		t.Error("mismatch")
	}

	entries, err = bt.GetLatestN(3)
	if err != nil {
		t.Error(err)
	}
	if len(entries) != 3 {
		t.Error("should only contain 3 entry but contained", len(entries))
		return
	}
	if string(entries[0].Value) != "testValue3" {
		t.Error("mismatch")
	}
	if string(entries[2].Value) != "testValue1" {
		t.Error("mismatch")
	}

	entries, err = bt.GetLatestN(0)
	if err != nil {
		t.Error(err)
	}
	if entries != nil {
		t.Error("as;lkdfj;adskfj")
	}

	entries, err = bt.GetLatestN(4)
	if err == nil {
		t.Error("expecting an error")
	}
	if entries != nil {
		t.Error("as;lkdfj;adskfj")
	}
}

func TestGetLatestN_Empty(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("test.db")

	entries, err := bt.GetLatestN(0)
	if err != nil {
		t.Error("Unexpected error: ", err)
	}
	if entries != nil {
		t.Error("Expected no entries but recieved ", len(entries))
	}
}

func TestGetLatestN_SingleEntry(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("test.db")

	testEntry := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put(testEntry)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	entries, err := bt.GetLatestN(1)
	if err != nil {
		t.Error("Unexpected error: ", err)
	}
	if len(entries) != 1 {
		t.Error("Should only contain 1 entry but contained", len(entries))
		return
	}
}

func TestGetLatestN_MultipleEntries(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("test.db")

	testEntry := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put(testEntry)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry2 := Entry{
		Time:  time.Now().Add(time.Minute),
		Value: []byte("testValue2"),
	}

	err = bt.Put(testEntry2)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry3 := Entry{
		Time:  time.Now().Add(time.Minute * 2),
		Value: []byte("testValue3"),
	}

	err = bt.Put(testEntry3)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	entries, err := bt.GetLatestN(3)
	if err != nil {
		t.Error("Unexpected error: ", err)
	}
	if len(entries) != 3 {
		t.Error("Should only contain 3 entry but contained", len(entries))
		return
	}
	if string(entries[0].Value) != "testValue3" {
		t.Error("Out of order error")
	}
	if string(entries[2].Value) != "testValue1" {
		t.Error("Out of order error")
	}
}

func TestGetLatestN_NLargerThanExists(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("test.db")

	testEntry := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put(testEntry)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	entries, err := bt.GetLatestN(4)
	if err == nil {
		t.Error("expecting an error")
	}
	if entries != nil {
		t.Error("Should return an empty Entry slice when more than exist are requested")
	}
}

func TestGetSince_NoEntries(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("test.db")

	entries, err := bt.GetSince(time.Now())
	if err != nil {
		t.Error("Unexpected Error:", err)
	}
	if entries != nil {
		t.Error("Shsould return an empty Entry slice when more than exist are requested")
	}
}

func TestGetSince_SingleEntry(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("test.db")

	testEntry := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put(testEntry)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	entries, err := bt.GetSince(time.Now().Add(-time.Minute * 10))
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(entries) != 1 {
		t.Error("Should return an empty Entry slice when more than exist are requested")
		return
	}
	if string(entries[0].Value) != "testValue1" {
		t.Error("Incorrect value returned")
	}
}

func TestGetSince_SingleEntryNotInRange(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("test.db")

	testEntry := Entry{
		Time:  time.Now().Add(-time.Minute * 10),
		Value: []byte("testValue1"),
	}

	err = bt.Put(testEntry)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	entries, err := bt.GetSince(time.Now())
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(entries) != 0 {
		t.Error("Should return an empty Entry slice when more than exist are requested")
		return
	}
}

func TestGetSince_MultiEntryOneInRange(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("test.db")

	testEntry1 := Entry{
		Time:  time.Now().Add(-time.Minute * 10),
		Value: []byte("testValue1"),
	}

	err = bt.Put(testEntry1)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry2 := Entry{
		Time:  time.Now().Add(-time.Minute * 5),
		Value: []byte("testValue2"),
	}

	err = bt.Put(testEntry2)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	entries, err := bt.GetSince(time.Now().Add(-time.Minute * 7))
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(entries) != 1 {
		t.Error("Should return an empty Entry slice when more than exist are requested")
		return
	}
	if string(entries[0].Value) != "testValue2" {
		t.Error("Incorrect value returned")
	}
}

func TestDeleteBefore_NoEntries(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("test.db")

	err = bt.DeleteBefore(time.Now())
	if err != nil {
		t.Error("Unexpected error:", err)
	}
}

func TestDeleteBefore_NoneBefore(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("test.db")

	testEntry := Entry{
		Time:  time.Now(),
		Value: []byte("testValue"),
	}

	err = bt.Put(testEntry)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	err = bt.DeleteBefore(time.Now().Add(-time.Minute * 5))
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	entries, err := bt.GetSince(time.Now().Add(-time.Minute * 100))
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(entries) != 1 {
		t.Error("Should return one entry")
		return
	}
}

func TestDeleteBefore_OneBefore(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("test.db")

	testEntry1 := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put(testEntry1)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry2 := Entry{
		Time:  time.Now().Add(-time.Minute * 10),
		Value: []byte("testValue2"),
	}

	err = bt.Put(testEntry2)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	err = bt.DeleteBefore(time.Now().Add(-time.Minute * 5))
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	entries, err := bt.GetSince(time.Now().Add(-time.Minute * 100))
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(entries) != 1 {
		t.Error("Should return one entry")
		return
	}
}

func TestDeleteBefore_MultipleBefore(t *testing.T) {
	bt, err := newBoltTimeDB("test.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("test.db")

	testEntry1 := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put(testEntry1)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry2 := Entry{
		Time:  time.Now().Add(-time.Minute * 10),
		Value: []byte("testValue2"),
	}

	err = bt.Put(testEntry2)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry3 := Entry{
		Time:  time.Now().Add(-time.Minute * 12),
		Value: []byte("testValue3"),
	}

	err = bt.Put(testEntry3)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	err = bt.DeleteBefore(time.Now().Add(-time.Minute * 5))
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	entries, err := bt.GetSince(time.Now().Add(-time.Minute * 100))
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(entries) != 1 {
		t.Error("Should return one entry")
		return
	}
}

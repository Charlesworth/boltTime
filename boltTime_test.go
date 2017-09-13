package boltTime

import (
	"os"
	"testing"
	"time"
)

func TestPut(t *testing.T) {
	bt, err := NewBoltTime("TestPut.db")
	if err != nil {
		t.Error()
	}
	defer os.Remove("TestPut.db")
	defer bt.DB.Close()
	testEntry := Entry{
		Time:  time.Now(),
		Value: []byte("testValue"),
	}

	err = bt.Put("testBucket", testEntry)
	if err != nil {
		t.Error()
	}
}

func TestGetLatestN_Empty(t *testing.T) {
	bt, err := NewBoltTime("TestGetLatestN_Empty.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("TestGetLatestN_Empty.db")
	defer bt.DB.Close()

	entries, err := bt.GetLatestN("testBucket", 0)
	if err != nil {
		t.Error("Unexpected error: ", err)
	}
	if entries != nil {
		t.Error("Expected no entries but recieved ", len(entries))
	}
}

func TestGetLatestN_SingleEntry(t *testing.T) {
	bt, err := NewBoltTime("TestGetLatestN_SingleEntry.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("TestGetLatestN_SingleEntry.db")
	defer bt.DB.Close()

	testEntry := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put("testBucket", testEntry)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	entries, err := bt.GetLatestN("testBucket", 1)
	if err != nil {
		t.Error("Unexpected error: ", err)
	}
	if len(entries) != 1 {
		t.Error("Should only contain 1 entry but contained", len(entries))
		return
	}
}

func TestGetLatestN_MultipleEntries(t *testing.T) {
	bt, err := NewBoltTime("TestGetLatestN_MultipleEntries.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("TestGetLatestN_MultipleEntries.db")
	defer bt.DB.Close()

	testEntry := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put("testBucket", testEntry)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry2 := Entry{
		Time:  time.Now().Add(time.Minute),
		Value: []byte("testValue2"),
	}

	err = bt.Put("testBucket", testEntry2)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry3 := Entry{
		Time:  time.Now().Add(time.Minute * 2),
		Value: []byte("testValue3"),
	}

	err = bt.Put("testBucket", testEntry3)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	entries, err := bt.GetLatestN("testBucket", 3)
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
	bt, err := NewBoltTime("TestGetLatestN_NLargerThanExists.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("TestGetLatestN_NLargerThanExists.db")
	defer bt.DB.Close()

	testEntry := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put("testBucket", testEntry)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	entries, err := bt.GetLatestN("testBucket", 4)
	if err != nil {
		t.Error("expecting an error")
	}

	if len(entries) != 1 {
		t.Error("Should return what was present when more than exist are requested")
	}
}

func TestGetSince_NoEntries(t *testing.T) {
	bt, err := NewBoltTime("TestGetSince_NoEntries.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("TestGetSince_NoEntries.db")
	defer bt.DB.Close()

	entries, err := bt.GetSince("testBucket", time.Now())
	if err != nil {
		t.Error("Unexpected Error:", err)
	}
	if entries != nil {
		t.Error("Shsould return an empty Entry slice when more than exist are requested")
	}
}

func TestGetSince_SingleEntry(t *testing.T) {
	bt, err := NewBoltTime("TestGetSince_SingleEntry.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("TestGetSince_SingleEntry.db")
	defer bt.DB.Close()

	testEntry := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put("testBucket", testEntry)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	entries, err := bt.GetSince("testBucket", time.Now().Add(-time.Minute*10))
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
	bt, err := NewBoltTime("TestGetSince_SingleEntryNotInRange.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("TestGetSince_SingleEntryNotInRange.db")
	defer bt.DB.Close()

	testEntry := Entry{
		Time:  time.Now().Add(-time.Minute * 10),
		Value: []byte("testValue1"),
	}

	err = bt.Put("testBucket", testEntry)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	entries, err := bt.GetSince("testBucket", time.Now())
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(entries) != 0 {
		t.Error("Should return an empty Entry slice when more than exist are requested")
		return
	}
}

func TestGetSince_MultiEntryOneInRange(t *testing.T) {
	bt, err := NewBoltTime("TestGetSince_MultiEntryOneInRange.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("TestGetSince_MultiEntryOneInRange.db")
	defer bt.DB.Close()

	testEntry1 := Entry{
		Time:  time.Now().Add(-time.Minute * 10),
		Value: []byte("testValue1"),
	}

	err = bt.Put("testBucket", testEntry1)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry2 := Entry{
		Time:  time.Now().Add(-time.Minute * 5),
		Value: []byte("testValue2"),
	}

	err = bt.Put("testBucket", testEntry2)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	entries, err := bt.GetSince("testBucket", time.Now().Add(-time.Minute*7))
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
	bt, err := NewBoltTime("TestDeleteBefore_NoEntries.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("TestDeleteBefore_NoEntries.db")
	defer bt.DB.Close()

	err = bt.DeleteBefore("testBucket", time.Now())
	if err != nil {
		t.Error("Unexpected error:", err)
	}
}

func TestDeleteBefore_NoneBefore(t *testing.T) {
	bt, err := NewBoltTime("TestDeleteBefore_NoneBefore.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("TestDeleteBefore_NoneBefore.db")
	defer bt.DB.Close()

	testEntry := Entry{
		Time:  time.Now(),
		Value: []byte("testValue"),
	}

	err = bt.Put("testBucket", testEntry)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	err = bt.DeleteBefore("testBucket", time.Now().Add(-time.Minute*5))
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	entries, err := bt.GetSince("testBucket", time.Now().Add(-time.Minute*100))
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(entries) != 1 {
		t.Error("Should return one entry")
		return
	}
}

func TestDeleteBefore_OneBefore(t *testing.T) {
	bt, err := NewBoltTime("TestDeleteBefore_OneBefore.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("TestDeleteBefore_OneBefore.db")
	defer bt.DB.Close()

	testEntry1 := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put("testBucket", testEntry1)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry2 := Entry{
		Time:  time.Now().Add(-time.Minute * 10),
		Value: []byte("testValue2"),
	}

	err = bt.Put("testBucket", testEntry2)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	err = bt.DeleteBefore("testBucket", time.Now().Add(-time.Minute*5))
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	entries, err := bt.GetSince("testBucket", time.Now().Add(-time.Minute*100))
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(entries) != 1 {
		t.Error("Should return one entry")
		return
	}
}

func TestDeleteBefore_MultipleBefore(t *testing.T) {
	bt, err := NewBoltTime("TestDeleteBefore_MultipleBefore.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("TestDeleteBefore_MultipleBefore.db")
	defer bt.DB.Close()

	testEntry1 := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put("testBucket", testEntry1)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry2 := Entry{
		Time:  time.Now().Add(-time.Minute * 10),
		Value: []byte("testValue2"),
	}

	err = bt.Put("testBucket", testEntry2)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry3 := Entry{
		Time:  time.Now().Add(-time.Minute * 12),
		Value: []byte("testValue3"),
	}

	err = bt.Put("testBucket", testEntry3)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	err = bt.DeleteBefore("testBucket", time.Now().Add(-time.Minute*5))
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	entries, err := bt.GetSince("testBucket", time.Now().Add(-time.Minute*100))
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(entries) != 1 {
		t.Error("Should return one entry")
		return
	}
}

func TestDeleteBefore_MultipleAfter(t *testing.T) {
	bt, err := NewBoltTime("TestDeleteBefore_MultipleAfter.db")
	if err != nil {
		t.Error("Test setup fail, unable to start DB:", err)
	}
	defer os.Remove("TestDeleteBefore_MultipleAfter.db")
	defer bt.DB.Close()

	testEntry1 := Entry{
		Time:  time.Now(),
		Value: []byte("testValue1"),
	}

	err = bt.Put("testBucket", testEntry1)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry2 := Entry{
		Time:  time.Now().Add(-time.Minute * 4),
		Value: []byte("testValue2"),
	}

	err = bt.Put("testBucket", testEntry2)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	testEntry3 := Entry{
		Time:  time.Now().Add(-time.Minute * 10),
		Value: []byte("testValue3"),
	}

	err = bt.Put("testBucket", testEntry3)
	if err != nil {
		t.Error("Test setup error, unable to put:", err)
	}

	err = bt.DeleteBefore("testBucket", time.Now().Add(-time.Minute*6))
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	entries, err := bt.GetSince("testBucket", time.Now().Add(-time.Minute*100))
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(entries) != 2 {
		t.Error("Should return one entry")
		return
	}
}

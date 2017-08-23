package boltTime

import (
	"time"

	"github.com/boltdb/bolt"
)

// TimeStore decribes the interface that BoltTime fulfils
type TimeStore interface {
	Put(bucket string, entry Entry) error
	GetSince(bucket string, t time.Time) ([]Entry, error)
	DeleteBefore(bucket string, t time.Time) error
	GetLatestN(bucket string, n int) ([]Entry, error)
}

// Entry contains a time and a []byte value
type Entry struct {
	Time  time.Time
	Value []byte
}

// BoltTime is a type that fulfils the TimeStore interface
type BoltTime struct {
	DB     *bolt.DB
	bucket string
}

const (
	defaultBucket = "default"
)

// NewBoltTime returns a initialised BoltTime instance
func NewBoltTime(dbFile string) (*BoltTime, error) {
	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		return nil, err
	}

	return &BoltTime{
		DB: db,
	}, nil
}

// Put puts a Entry into the datastore in the specified bucket
func (bt *BoltTime) Put(bucket string, entry Entry) error {
	return bt.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		err = b.Put([]byte(entry.Time.Format(time.RFC3339)), entry.Value)
		if err != nil {
			return err
		}

		return nil
	})
}

// DeleteBefore deletes all entries before time t
func (bt *BoltTime) DeleteBefore(bucket string, t time.Time) error {
	return bt.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return nil
		}

		c := b.Cursor()

		k, _ := c.First()

		for k != nil {
			entryT, err := time.Parse(time.RFC3339, string(k))
			if err != nil {
				return err
			}

			if entryT.Before(t) {
				c.Delete()
			} else {
				return nil
			}

			k, _ = c.Next()
		}

		return nil
	})
}

// GetSince returns all values since time t
func (bt *BoltTime) GetSince(bucket string, t time.Time) (entries []Entry, err error) {
	err = bt.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return nil
		}

		c := b.Cursor()

		startTime := []byte(t.Format(time.RFC3339))

		for k, v := c.Seek(startTime); k != nil; k, v = c.Next() {
			t, err := time.Parse(time.RFC3339, string(k))
			if err != nil {
				return err
			}

			entries = append(entries, Entry{
				Time:  t,
				Value: v,
			})
		}

		return nil
	})

	return entries, err
}

// GetLatestN retrieves n most recent entries
func (bt *BoltTime) GetLatestN(bucket string, n int) (entries []Entry, err error) {
	err = bt.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return nil
		}

		c := b.Cursor()

		i := 0
		for k, v := c.Last(); i < n; k, v = c.Prev() {
			t, err := time.Parse(time.RFC3339, string(k))
			if err != nil {
				return err
			}

			entries = append(entries, Entry{
				Time:  t,
				Value: v,
			})

			i++
		}

		return nil
	})

	return entries, err
}

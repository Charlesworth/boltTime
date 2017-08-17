package boltTime

import (
	"errors"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

type TimeStore interface {
	Put(Entry) error
	GetSince(time.Time) ([]Entry, error)
	DeleteBefore(time.Time) error
	GetLatestN(n int) ([]Entry, error)
}

type Entry struct {
	Time  time.Time
	Value []byte
}

type BoltTimeDB struct {
	DB *bolt.DB
}

const (
	bucket = "default"
)

func newBoltTimeDB(dbFile string) (*BoltTimeDB, error) {
	// Open the <dbFile>.db data file in your current directory.
	// It will be created if it doesn't exist.
	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		return nil, err
	}

	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		return nil
	})

	return &BoltTimeDB{
		DB: db,
	}, nil
}

func (bt *BoltTimeDB) Put(entry Entry) error {
	return bt.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))

		err := bucket.Put([]byte(entry.Time.Format(time.RFC3339)), entry.Value)
		if err != nil {
			return err
		}

		return nil
	})
}

func (bt *BoltTimeDB) GetSince(t time.Time) (entries []Entry, err error) {
	err = bt.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))

		c := bucket.Cursor()

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

func (bt *BoltTimeDB) DeleteBefore(t time.Time) error {
	return bt.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))

		c := bucket.Cursor()

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

func (bt *BoltTimeDB) GetLatestN(n int) (entries []Entry, err error) {
	err = bt.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))
		if err != nil {
			return err
		}

		if bucket.Stats().KeyN < n {
			return errors.New("boltStore contians " + fmt.Sprint(bucket.Stats().KeyN) + " entries while " + fmt.Sprint(n) + " were requested")
		}

		c := bucket.Cursor()

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

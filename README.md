# boltTime
Time/Value database built on boltDB

### Interface:

	Put(bucket string, entry Entry) error
	GetSince(bucket string, t time.Time) ([]Entry, error)
	DeleteBefore(bucket string, t time.Time) error
	GetLatestN(bucket string, n int) ([]Entry, error)

To get a new BoltTime instance:

        NewBoltTime(dbFile string) (*BoltTime, error)
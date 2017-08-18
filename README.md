# boltTime
Time/Value database built on boltDB

### Interface:

        Put(Entry) error
        GetSince(time.Time) ([]Entry, error)
        DeleteBefore(time.Time) error
        GetLatestN(n int) ([]Entry, error)

To get a new BoltTime instance:

        NewBoltTime(dbFile string) (*BoltTime, error)
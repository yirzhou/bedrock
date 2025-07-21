package lib

var (
	TOMBSTONE  = []byte("__TOMBSTONE__")
	CHECKPOINT = []byte("__CHECKPOINT__")
)

var TXN_COMMIT = byte(0x02)
var TXN_PUT = byte(0x01)

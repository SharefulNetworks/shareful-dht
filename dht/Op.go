package dht

// op type enum-like
const (
	OP_STORE        = 1
	OP_FIND         = 2
	OP_STORE_INDEX  = 3
	OP_FIND_INDEX   = 4
	OP_PING         = 5
	OP_CONNECT      = 6
	OP_DELETE_INDEX = 7
	OP_FIND_NODE    = 8
	OP_FIND_VALUE   = 9 // NEW
	OP_SYNC_INDEX   = 10
)

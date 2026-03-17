package commons

type MessageHeaderLike interface {
	GetKey() string
	GetValue() string
}
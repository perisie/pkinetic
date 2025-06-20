package pkinetic

type Pkinetic interface {
	Creator
	Getter
}

type Creator interface {
	Create(partition_key string, sort_key string, data map[string]string) error
}

type Getter interface {
	Get(partition_key string, prefix string) ([]*Item, error)
}

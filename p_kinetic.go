package pkinetic

type Pkinetic interface {
	Creator
	Getter
	Updater
	Deleter
}

type Creator interface {
	Create(partition_key string, sort_key string, data map[string]string) error
}

type Getter interface {
	Get(partition_key string, prefix string) ([]*Item, error)
}

type Updater interface {
	Update(partition_key string, sort_key string, update map[string]string) error
}

type Deleter interface {
	Delete(partition_key string, sort_key string) error
}

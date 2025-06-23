package pkinetic

type Pkinetic interface {
	Creator
	Getter
	Updater
	Deleter
}

type Creator interface {
	Create(partition_key string, sort_key string, data map[string]string) (*Item, error)
}

type Getter interface {
	Get(partition_key string, prefix string) ([]*Item, error)
	Get_single(partition_key string, sort_key string) (*Item, error)
	Get_gsi(
		index string,
		index_partition_key_name string,
		index_partition_key_value string,
		index_sort_key_name string,
		index_sort_key_prefix string,
	) ([]*Item, error)
}

type Updater interface {
	Update(partition_key string, sort_key string, update map[string]string) error
}

type Deleter interface {
	Delete(partition_key string, sort_key string) error
}

const (
	aws_access_key_id     = "AWS_ACCESS_KEY_ID"
	aws_secret_access_key = "AWS_SECRET_ACCESS_KEY"
)

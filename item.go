package pkinetic

type Item struct {
	partition_key string
	sort_key      string
	data          map[string]string
}

func (i *Item) Get_partition_key() string {
	return i.partition_key
}

func (i *Item) Get_sort_key() string {
	return i.sort_key
}

func (i *Item) Get_data() map[string]string {
	data := map[string]string{}
	for k, v := range i.data {
		data[k] = v
	}
	return data
}

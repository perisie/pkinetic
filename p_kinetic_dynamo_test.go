package pkinetic

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_gsi(t *testing.T) {
	pkinetic_dynamo, err := Pkinetic_dynamo_new(
		"ap-southeast-5",
		"pkinetic-dev",
	)
	require.Nil(t, err)

	dob := time.Now().Format(time.RFC3339Nano)

	var creator Creator = pkinetic_dynamo
	_ = creator.Create(
		uuid.New().String(),
		uuid.New().String(),
		map[string]string{
			"name": "A",
			"dob":  dob,
		},
	)
	_ = creator.Create(
		uuid.New().String(),
		uuid.New().String(),
		map[string]string{
			"name": "A",
			"dob":  dob,
		},
	)
	_ = creator.Create(
		uuid.New().String(),
		uuid.New().String(),
		map[string]string{
			"name": "B",
			"dob":  dob,
		},
	)

	var getter Getter = pkinetic_dynamo
	index := "dob-name-index"
	index_partition_key_name := "dob"
	index_partition_key_value := dob
	index_sort_key_name := "name"
	index_sort_key_prefix := "A"
	items, err := getter.Get_gsi(
		index,
		index_partition_key_name,
		index_partition_key_value,
		index_sort_key_name,
		index_sort_key_prefix,
	)
	require.Nil(t, err)
	require.Len(t, items, 2)
	for _, item := range items {
		require.Equal(t, index_partition_key_value, item.Get_data()["dob"])
		require.Equal(t, index_sort_key_prefix, item.Get_data()["name"])
	}

	items, err = getter.Get_gsi(
		index,
		index_partition_key_name,
		index_partition_key_value,
		index_sort_key_name,
		"",
	)
	require.Nil(t, err)
	require.Len(t, items, 3)
}

func Test(t *testing.T) {
	pkinetic_dynamo, err := Pkinetic_dynamo_new(
		"ap-southeast-5",
		"pkinetic-dev",
	)
	require.Nil(t, err)

	var creator Creator = pkinetic_dynamo
	partition_key := uuid.New().String()
	sort_key := uuid.New().String()
	err = creator.Create(partition_key, sort_key, map[string]string{
		"name": "Aishah",
		"dob":  "1998-11-01",
	})
	require.Nil(t, err)

	err = creator.Create(partition_key, sort_key, map[string]string{
		"name": "A",
		"dob":  "0000-00-00",
	})
	require.NotNil(t, err)

	err = creator.Create(partition_key, sort_key+"-2", map[string]string{
		"name": "B",
		"dob":  "0000-00-00",
	})
	require.Nil(t, err)

	var getter Getter = pkinetic_dynamo
	items, err := getter.Get(partition_key, "")
	require.Nil(t, err)
	require.Len(t, items, 2)

	require.Equal(t, partition_key, items[0].Get_partition_key())
	require.Equal(t, sort_key, items[0].Get_sort_key())

	require.Equal(t, partition_key, items[1].Get_partition_key())
	require.Equal(t, sort_key+"-2", items[1].Get_sort_key())

	var updater Updater = pkinetic_dynamo
	err = updater.Update(partition_key, sort_key, map[string]string{
		"name": "Ecah",
	})
	require.Nil(t, err)

	items, err = getter.Get(partition_key, sort_key)
	require.Nil(t, err)
	require.Len(t, items, 2)

	require.Equal(t, partition_key, items[0].Get_partition_key())
	require.Equal(t, sort_key, items[0].Get_sort_key())
	require.Equal(t, "Ecah", items[0].Get_data()["name"])

	require.Equal(t, partition_key, items[1].Get_partition_key())
	require.Equal(t, sort_key+"-2", items[1].Get_sort_key())
	require.Equal(t, "B", items[1].Get_data()["name"])

	item, err := getter.Get_single(partition_key, sort_key)
	require.Nil(t, err)
	require.Equal(t, partition_key, item.Get_partition_key())
	require.Equal(t, sort_key, item.Get_sort_key())
	require.Equal(t, "Ecah", item.Get_data()["name"])

	var deleter Deleter = pkinetic_dynamo
	err = deleter.Delete(partition_key, sort_key)
	require.Nil(t, err)

	items, err = getter.Get(partition_key, sort_key)
	require.Nil(t, err)
	require.Len(t, items, 1)

	require.Equal(t, partition_key, items[0].Get_partition_key())
	require.Equal(t, sort_key+"-2", items[0].Get_sort_key())
	require.Equal(t, "B", items[0].Get_data()["name"])

	err = updater.Update(partition_key, sort_key, map[string]string{
		"name": "Ecah",
	})
	require.NotNil(t, err)

	items, err = getter.Get(partition_key, sort_key)
	require.Nil(t, err)
	require.Len(t, items, 1)

	require.Equal(t, partition_key, items[0].Get_partition_key())
	require.Equal(t, sort_key+"-2", items[0].Get_sort_key())
	require.Equal(t, "B", items[0].Get_data()["name"])

	err = deleter.Delete(partition_key, sort_key+"-2")
	require.Nil(t, err)

	items, err = getter.Get(partition_key, sort_key)
	require.Nil(t, err)
	require.Len(t, items, 0)
}

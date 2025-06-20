package pkinetic

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test(t *testing.T) {
	var creator Creator
	creator, err := Pkinetic_dynamo_new(
		"ap-southeast-5",
		"pkinetic-dev",
	)
	require.Nil(t, err)

	partition_key := uuid.New().String()
	sort_key := uuid.New().String()
	err = creator.Create(partition_key, sort_key, map[string]string{
		"name": "Aishah",
		"dob":  "1998-11-01",
	})
	require.Nil(t, err)

	err = creator.Create(partition_key, sort_key, map[string]string{
		"name": "",
		"dob":  "",
	})
	require.NotNil(t, err)

	err = creator.Create(partition_key, sort_key+"-2", map[string]string{
		"name": "",
		"dob":  "",
	})
	require.Nil(t, err)

	var getter Getter
	getter, err = Pkinetic_dynamo_new(
		"ap-southeast-5",
		"pkinetic-dev",
	)
	require.Nil(t, err)

	items, err := getter.Get(partition_key, sort_key)
	require.Nil(t, err)
	require.Len(t, items, 2)

	require.Equal(t, partition_key, items[0].Get_partition_key())
	require.Equal(t, sort_key, items[0].Get_sort_key())

	require.Equal(t, partition_key, items[1].Get_partition_key())
	require.Equal(t, sort_key+"-2", items[1].Get_sort_key())
}

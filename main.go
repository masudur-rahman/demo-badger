package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/dgraph-io/badger/v3"
)

func main() {
	opts := badger.DefaultOptions("/tmp/badger")
	opts.WithMaxLevels(1)
	db, err := badger.Open(opts)
	handleError(err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	key := "events.s216.user.2.cluster.uid.product.kubedb"
	value := map[string]string{
		"hello": "hi",
		"bye":   "bye",
	}
	err = txn.Set(bytes(key), bytes(value))
	handleError(err)

	item, err := txn.Get(bytes(key))
	handleError(err)

	key = "events.s216.user.2"
	value = map[string]string{
		"hello": "hiii",
		"bye":   "byeee",
	}
	err = txn.Set(bytes(key), bytes(value))
	handleError(err)

	var data []byte
	data, err = item.ValueCopy(data)
	handleError(err)

	fmt.Println(string(data))

	err = txn.Set(bytes("int"), bytes(1))
	handleError(err)

	item, err = txn.Get(bytes("int"))
	handleError(err)
	data, err = item.ValueCopy(data)
	handleError(err)

	fmt.Println(string(data))
	handleError(err)

	//txn = db.NewTransaction(true)

	entry := badger.NewEntry(bytes("anything"), bytes("nothing")).WithTTL(time.Second * 2)
	err = txn.SetEntry(entry)
	handleError(err)

	item, err = txn.Get(bytes("anything"))
	handleError(err)

	data, err = item.ValueCopy(data)
	handleError(err)

	fmt.Println(string(item.Key()), "<==>", string(data))
	time.Sleep(time.Second * 2)
	item, err = txn.Get(bytes("anything"))
	if err != nil {
		fmt.Println(err)
	}
	if err == nil {
		data, err = item.ValueCopy(data)
		handleError(err)

		fmt.Println(string(item.Key()), "<==>", string(data))
	}

	seq, err := db.GetSequence(bytes("hello"), 10)
	handleError(err)
	defer seq.Release()

	for idx := 0; idx < 10; idx++ {
		num, err := seq.Next()
		handleError(err)
		fmt.Println(num)
	}

	key = "merge"

	m := db.GetMergeOperator(bytes(key), add, 200*time.Millisecond)
	defer m.Stop()
	m.Add(bytes("A"))
	m.Add(bytes("B"))
	m.Add(bytes("C"))

	res, err := m.Get()
	handleError(err)
	fmt.Println(string(res))

	err = txn.Commit()
	handleError(err)

	txn = db.NewTransaction(false)
	itOpts := badger.DefaultIteratorOptions
	itOpts.PrefetchSize = 10
	it := txn.NewIterator(itOpts)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		item = it.Item()
		data, err = item.ValueCopy(data)
		handleError(err)
		fmt.Printf("key = %s, value = %s\n", item.Key(), data)

		itt := txn.NewKeyIterator(item.Key(), itOpts)
		for itt.Rewind(); itt.Valid(); itt.Next() {
			item = itt.Item()
			data, err = item.ValueCopy(data)
			handleError(err)
			fmt.Printf("\t\tkey = %s, value = %s, version = %v\n", item.Key(), data, item.Version())

		}
	}

	fmt.Println("\n***************\nPrefix Scans\n**************")

	prefix := "events."
	itOpts.PrefetchValues = false
	it = txn.NewIterator(itOpts)

	for it.Seek(bytes(prefix)); it.ValidForPrefix(bytes(prefix)); it.Next() {
		item = it.Item()
		data, err = item.ValueCopy(data)
		handleError(err)
		fmt.Printf("key = %s, value = %s\n", item.Key(), data)
	}
	err = txn.Commit()
	handleError(err)

	file, err := os.OpenFile("/tmp/backup.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	handleError(err)

	_, err = db.Backup(file, 0)
	handleError(err)
}

// Merge function to append one byte slice to another
func add(originalValue, newValue []byte) []byte {
	return append(originalValue, newValue...)
}

func bytes(obj interface{}) []byte {
	switch data := obj.(type) {
	case []byte:
		return data
	case string:
		return []byte(data)
	default:
		d, err := json.Marshal(data)
		if err != nil {
			panic(err)
		}
		return d
	}
}

func handleError(err error) {
	if err != nil {
		panic(err)
	}
}

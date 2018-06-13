/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fsblkstorage

import (
	"github.com/colinmarc/hdfs"
	"time"
)

////  WRITER ////
type blockfileWriter struct {
	filePath string
	//file     *os.File
	file	*hdfs.FileWriter
	client	*hdfs.Client
}

func newBlockfileWriter(filePath string) (*blockfileWriter, error) {
	writer := &blockfileWriter{filePath: filePath}
	return writer, writer.open()
}

func (w *blockfileWriter) truncateFile(targetSize int) error {
	//
	fileStat, err := w.client.Stat(w.filePath)
	if err != nil {
		return err
	}
	if fileStat.Size() > int64(targetSize) {
		//w.file.Truncate(int64(targetSize))
		err = w.file.Close()
		if err != nil {
			return err
		}
		if err = w.client.Truncate(w.filePath, uint64(targetSize)); err != nil {
			return err
		}
		time.Sleep(3 * time.Second)
		file, err := w.client.Append(w.filePath)
		if err != nil {
			logger.Debugf("Error while append block file [%s]", err)
			return err
		}
		w.file = file
	}
	return nil
}

func (w *blockfileWriter) append(b []byte, sync bool) error {
	_, err := w.file.Write(b)
	if err != nil {
		return err
	}
	err = w.file.Flush()
	if err != nil {
		return err
	}
	err = w.file.Close()
	if err != nil {
		return err
	}
	file, err := w.client.Append(w.filePath)
	if err != nil {
		logger.Debugf("Error while append block file [%s]", err)
		return err
	}
	w.file = file
	//
	//if sync {
	//	return w.file.Sync()
	//}
	return nil
}

func (w *blockfileWriter) open() error {
	//file, err := os.OpenFile(w.filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	client, err := hdfs.New(hdfsHost)
	if err != nil {
		logger.Debugf("Error while creating hdfs client [%s]", err)
		return err
	}
	file, err := client.Create(w.filePath)
	if err != nil {
		logger.Debugf("Error while creating hdfs file [%s], then trying open", err)
		file, err = client.Append(w.filePath)
		if err != nil {
			return err
		}
	}
	w.file = file
	w.client = client
	return nil
}

func (w *blockfileWriter) close() error {
	err := w.file.Close()
	if err != nil {
		return err
	}
	err = w.client.Close()
	if err != nil {
		return err
	}
	return nil
}

////  READER ////
type blockfileReader struct {
	//file *os.File
	file *hdfs.FileReader
	client *hdfs.Client
}

func newBlockfileReader(filePath string) (*blockfileReader, error) {
	//
	client, err := hdfs.New(hdfsHost)
	if err != nil {
		logger.Debugf("Error while creating hdfs client [%s]", err)
		return nil, err
	}
	file, err := client.Open(filePath)
	//file, err := os.OpenFile(filePath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	reader := &blockfileReader{file, client}
	return reader, nil
}

func (r *blockfileReader) read(offset int, length int) ([]byte, error) {
	b := make([]byte, length)
	_, err := r.file.ReadAt(b, int64(offset))
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (r *blockfileReader) close() error {
	err := r.file.Close()
	if err != nil {
		return err
	}
	err = r.client.Close()
	if err != nil {
		return err
	}
	return nil
}

package utils

import (
	"encoding/csv"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func AssertnumResult(t *testing.T, want, get int64) {
	t.Helper()
	if want != get {
		t.Errorf("want get %v, actual get %v\n", want, get)
	}
}

func AssertStringResult(t *testing.T, want, get string) {
	t.Helper()
	if want != get {
		t.Errorf("want get %v, actual get %v\n", want, get)
	}
}

func AssertfloatResult(t *testing.T, want, get float64) {
	t.Helper()
	if want != get {
		t.Errorf("want get %v, actual get %v\n", want, get)
	}
}

func AssertThread(t *testing.T, threadStat int32) {
	t.Helper()
	if threadStat != 0 {
		t.Error("The clean sessions thread is still alive?!?")
	}
}

func AssertFalse(t *testing.T, v bool) {
	t.Helper()
	if v == true {
		t.Error("assert false but get a true value")
	}
}

func AssertTrue(t *testing.T, v bool) {
	t.Helper()
	if v != true {
		t.Error("assert false but get a true value")
	}
}

func RandomFloat(start, end float64) float64 {
	return start + float64(rand.Int63n(int64(end-start)))
}

func CSVReader(filename string) [][]string {
	csvfile, err := os.Open(filename)
	if err != nil {
		log.Fatalf("open file fault, filename: %s, err: %v", filename, err)
	}
	file := csv.NewReader(csvfile)
	var res [][]string
	for {
		record, err := file.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalln("read csv fault, err: ", err)
		}
		res = append(res, record)
	}
	return res
}

func IsDigital(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return false
	}
	return true
}

func RandomString(up int) string {
	rand.Seed(rand.Int63())
	return strconv.Itoa(rand.Intn(up))
}

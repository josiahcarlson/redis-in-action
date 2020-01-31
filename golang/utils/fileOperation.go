package utils

import (
	"archive/zip"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func GenerationZipFile(path, filename string) *os.File {
	//outFile, err := os.Create("temp-3.zip")
	outFile, err := os.Create(path +"/" + filename)
	if err != nil {
		log.Fatal(err)
	}
	zipWriter := zip.NewWriter(outFile)
	fileWriter, err := zipWriter.Create("temp-3.txt")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		_, err = fileWriter.Write([]byte(strings.Repeat(
			fmt.Sprintf("random line %s\n", strconv.Itoa(i)), 10)))
		if err != nil {
			log.Fatal(err)
		}
	}
	_ = zipWriter.Close()
	return outFile
}

func GenerationFile(tempDirPath, fileName, content string) *os.File {
	tempFile, err := os.Create(tempDirPath + "/"+ fileName)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := tempFile.WriteString(content); err != nil {
		log.Println("write to file err: ", err)
		return nil
	}
	return tempFile
}

func CleanFile(tempFile *os.File) {
	_ = tempFile.Close()
	_ = os.Remove(tempFile.Name())
}


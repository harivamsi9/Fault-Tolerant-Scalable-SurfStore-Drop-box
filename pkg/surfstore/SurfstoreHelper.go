package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT,
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()

	//panic("todo")

	curr_statement, err := db.Prepare(insertTuple)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}

	for _, curr_fileMeta := range fileMetas {
		curr_fileName := curr_fileMeta.GetFilename()
		curr_version := curr_fileMeta.GetVersion()
		curr_hashIndex := -1
		for _, curr_hashValue := range curr_fileMeta.BlockHashList {
			curr_hashIndex = curr_hashIndex + 1
			curr_statement.Exec(curr_fileName, curr_version, curr_hashIndex, curr_hashValue)
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select DISTINCT fileName, version from indexes;`

const getTuplesByFileName string = `select hashValue from indexes where fileName = ? order by hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)

	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}

	//panic("todo")
	curr_statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error occured in: Meta Write Back")
	}
	curr_statement.Exec()

	listOfDistinctFileNames, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Fatal("Error occured in: Querying Meta")
	}
	for listOfDistinctFileNames.Next() {
		var curr_fileName string
		var curr_version int
		listOfDistinctFileNames.Scan(&curr_fileName, &curr_version)
		listOftuplesByFileName, err := db.Query(getTuplesByFileName, curr_fileName)
		if err != nil {
			log.Fatal("Error occured in: Querying Meta")
		}
		var blockHashList []string
		for listOftuplesByFileName.Next() {
			var hashValue string
			listOftuplesByFileName.Scan(&hashValue)
			blockHashList = append(blockHashList, hashValue)
		}
		fileMetaMap[curr_fileName] = &FileMetaData{
			Filename:      curr_fileName,
			Version:       int32(curr_version),
			BlockHashList: blockHashList,
		}
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}

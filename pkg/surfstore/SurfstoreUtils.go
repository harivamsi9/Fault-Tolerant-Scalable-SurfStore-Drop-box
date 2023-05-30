package surfstore

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	reflect "reflect"
)

func performForEveryServer(client RPCClient, blockStoreMap map[string][]string, hashMap map[string][]byte, counter int32) {
	for serverAddr := range blockStoreMap {
		blockhashes := blockStoreMap[serverAddr]
		for _, blockhash := range blockhashes {
			if blockhash != EMPTYFILE_HASHVALUE {
				blockBytes := hashMap[blockhash]
				block := Block{BlockData: blockBytes, BlockSize: int32(len(blockBytes))}
				var succ bool
				if err := client.PutBlock(&block, serverAddr, &succ); err != nil {
					log.Println("ERROR OCCURED WHILE PUTTING CURR_BLOCK", err)
				}
				if succ {
					counter += 1
				}
			}
		}
	}
}

func computeHashList(file_To_Read *os.File, BlockSize int, hashMap map[string][]byte, hashList []string) ([]string, map[string][]byte, error) {
	reader := bufio.NewReaderSize(file_To_Read, BlockSize)
	for {
		buf := make([]byte, BlockSize)
		n, err := reader.Read(buf)

		if err != nil {
			if err != io.EOF {
				log.Println(err.Error())
			}
			break
		}

		blockBytes := buf[:n]
		hash := GetBlockHashString(blockBytes)
		hashMap[hash] = blockBytes
		hashList = append(hashList, hash)
	}
	return hashList, hashMap, nil
}

func getFilesToBeDownloaded(client RPCClient, remoteIndex map[string]*FileMetaData) []string {
	filesToBeDownloaded := []string{}
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("ERROR IN LOADING META FROM META eachFile: ", err)
	}

	for eachRemoteFile := range remoteIndex {
		_, ok := localIndex[eachRemoteFile]
		if !ok {
			filesToBeDownloaded = append(filesToBeDownloaded, eachRemoteFile)
		} else {
			if !(remoteIndex[eachRemoteFile].BlockHashList[0] == TOMBSTONE_HASHVALUE && len(remoteIndex[eachRemoteFile].BlockHashList[0]) == 1) && remoteIndex[eachRemoteFile].Version > localIndex[eachRemoteFile].Version {
				filesToBeDownloaded = append(filesToBeDownloaded, eachRemoteFile)
			}
		}
	}

	return filesToBeDownloaded
}

func getFilesLocallyDeleted(client RPCClient, remoteIndex map[string]*FileMetaData, fileContents map[string][]string) []string {
	filesLocallyDeleted := []string{}
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("ERROR IN LOADING META FROM META eachFile: ", err)
	}

	for eachFile := range localIndex {
		_, isOk := fileContents[eachFile]
		if !isOk {
			isLocalFileBHL_len1 := len(localIndex[eachFile].BlockHashList) == 1
			isLocalFileBHL_null := localIndex[eachFile].BlockHashList[0] == TOMBSTONE_HASHVALUE
			if !(isLocalFileBHL_len1 && isLocalFileBHL_null) {
				filesLocallyDeleted = append(filesLocallyDeleted, eachFile)
			}
		}
	}

	return filesLocallyDeleted
}

func getFilesRemotelyDeleted(client RPCClient, remoteIndex map[string]*FileMetaData) []string {
	filesRemotelyDeleted := []string{}
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("ERROR IN LOADING META FROM META eachFile: ", err)
	}
	for eachFile := range remoteIndex {
		_, isOk := localIndex[eachFile]

		if isOk {
			if !(len(localIndex[eachFile].BlockHashList) == 1 && localIndex[eachFile].BlockHashList[0] == TOMBSTONE_HASHVALUE) && len(remoteIndex[eachFile].BlockHashList) == 1 && remoteIndex[eachFile].BlockHashList[0] == TOMBSTONE_HASHVALUE {
				filesRemotelyDeleted = append(filesRemotelyDeleted, eachFile)
			}
		}
	}
	return filesRemotelyDeleted
}
func update_metadataVersion(client RPCClient, newMetaData *FileMetaData, latest_Version int32) error {
	err := client.UpdateFile(newMetaData, &latest_Version)
	if err != nil {
		log.Println("ERROR IN UPDATING FILE: ", err)
		// newMetaData.Version = -1
	}
	return nil
	// newMetaData.Version = latest_Version
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("ERROR DURING READING BASEDIR OPERATION: ", err)
	}
	// loading meta from basedir
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("ERROR IN LOADING META FROM META eachFile: ", err)
	}

	hashMap := make(map[string][]byte)
	fileContents := make(map[string][]string)
	for _, eachFile := range files {
		eachFileName := eachFile.Name()
		isFileIndexDB := eachFileName == "index.db"
		if isFileIndexDB {
			continue
		}

		// Open the file
		client_baseDir := client.BaseDir
		path := client_baseDir + "/" + eachFileName
		file_To_Read, err := os.Open(path)
		if err != nil {
			log.Println("ERROR IN READING", eachFileName, "FROM BASEDIR:", err)
			continue
		}
		defer file_To_Read.Close()

		// Get the file size
		fi, err := file_To_Read.Stat()
		if err != nil {
			log.Println("ERROR IN GETTING FILE SIZE FOR", eachFileName, "FROM BASEDIR:", err)
			continue
		}
		fileSize := fi.Size()

		// Create an empty hashList for the file
		hashList := []string{}

		// If the file is empty, add the EMPTYFILE_HASHVALUE to the hashList and continue with the next file
		if fileSize == 0 {
			hashList = append(hashList, EMPTYFILE_HASHVALUE)
			fileContents[eachFile.Name()] = hashList
			continue
		}

		// Read the file in chunks and compute the hashList

		hashList, hashMap, err = computeHashList(file_To_Read, client.BlockSize, hashMap, hashList)
		if err != nil {
			log.Panicln("ERROR in computeHashList():", err)
		}

		// Add the hashList to the fileContents map
		fileContents[eachFile.Name()] = hashList
	}

	remoteIndex := make(map[string]*FileMetaData)
	err2 := client.GetFileInfoMap(&remoteIndex)
	if err2 != nil {
		log.Println("ERROR IN GETTING INDEX FROM SERVER: ", err)
	}

	filesToBeDownloaded := getFilesToBeDownloaded(client, remoteIndex)
	filesLocallyDeleted := getFilesLocallyDeleted(client, remoteIndex, fileContents)
	// sync.Mutex.Lock()
	filesRemotelyDeleted := getFilesRemotelyDeleted(client, remoteIndex)

	newlyAddedFiles := []string{}
	curr_modifiedFiles := []string{}

	for fileName := range fileContents {
		localMetaData, ok := localIndex[fileName]
		if ok {
			isReflectdeepEqual := reflect.DeepEqual(localMetaData.BlockHashList, fileContents[fileName])
			if isReflectdeepEqual {
				continue
			} else {

				if !isPresent(filesRemotelyDeleted, fileName) {
					curr_modifiedFiles = append(curr_modifiedFiles, fileName)
				}
			}
		} else {
			newlyAddedFiles = append(newlyAddedFiles, fileName)
		}
	}
	del_remotelyDeletedFiles(client, filesRemotelyDeleted)

	for _, eachNewlyAddedFile := range newlyAddedFiles {
		var counter int32
		hlist := fileContents[eachNewlyAddedFile]
		var blockStoreMap map[string][]string
		err := client.GetBlockStoreMap(hlist, &blockStoreMap)
		if err != nil {
			log.Println("ERROR: ", err)
		}
		performForEveryServer(client, blockStoreMap, hashMap, counter)
		latest_Version := int32(-1)
		newFileMetaData := &FileMetaData{Filename: eachNewlyAddedFile, Version: +1, BlockHashList: fileContents[eachNewlyAddedFile]}
		err = client.UpdateFile(newFileMetaData, &latest_Version)
	}

	for _, eachModifiedFile := range curr_modifiedFiles {
		var counter int32
		hlist := fileContents[eachModifiedFile]
		var blockStoreMap map[string][]string
		err := client.GetBlockStoreMap(hlist, &blockStoreMap)
		if err != nil {
			log.Println("ERROR IN GETTING BLOCKSTOREMAP VALUE: ", err)
		}

		performForEveryServer(client, blockStoreMap, hashMap, counter)

		latest_Version := int32(-1)
		newMetaData := &FileMetaData{Filename: eachModifiedFile, Version: remoteIndex[eachModifiedFile].Version + 1, BlockHashList: fileContents[eachModifiedFile]}
		update_metadataVersion(client, newMetaData, latest_Version)

		if latest_Version == -1 {
			log.Println("WARNING: ENCOUNTERED WRITE-WRITE CONFLICT")
		}
	}

	for _, eachLocallyDeletedFile := range filesLocallyDeleted { //for every new file
		latest_Version := int32(remoteIndex[eachLocallyDeletedFile].Version + 1)
		newMetaData := &FileMetaData{Filename: eachLocallyDeletedFile, Version: latest_Version, BlockHashList: []string{TOMBSTONE_HASHVALUE}}
		update_metadataVersion(client, newMetaData, latest_Version)
	}

	// check remote, and download
	for _, filename := range filesToBeDownloaded {
		remote_fileDownload := remoteIndex[filename].Filename
		remote_fileDownload_BHL := remoteIndex[filename].BlockHashList
		// BLK,  := getBlockBytes(client, remote_fileDownload_BHL)
		blockBytes, err := getBlockBytes(client, remote_fileDownload_BHL)
		if err != nil {
			log.Printf("Error getting block bytes: %s", err)
		}

		writeIntoFile(client.BaseDir, remote_fileDownload, blockBytes)
	}

	remoteIndex = make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteIndex)
	if err != nil {
		log.Println("ERROR: ", err)
		// newMetaData.Version = -1
	}
	// checkError(err)
	err = WriteMetaFile(remoteIndex, client.BaseDir)
	if err != nil {
		log.Println("ERROR: ", err)
		// newMetaData.Version = -1
	}

}

func getBlockBytes(client RPCClient, hlist []string) ([]byte, error) {
	var buf bytes.Buffer
	for _, h := range hlist {
		block := &Block{}
		blockhashes := []string{h}
		var blockStoreMap map[string][]string
		err := client.GetBlockStoreMap(blockhashes, &blockStoreMap)
		if err != nil {
			return nil, fmt.Errorf("error getting block store map: %s", err)
		}
		for serverAddr := range blockStoreMap {
			serverBlockHashes := blockStoreMap[serverAddr]
			for _, blockhash := range serverBlockHashes {
				err := client.GetBlock(blockhash, serverAddr, block)
				if err != nil {
					return nil, fmt.Errorf("error getting block %s from server %s: %s", blockhash, serverAddr, err)
				}
				if _, err := buf.Write(block.BlockData); err != nil {
					return nil, fmt.Errorf("error writing block data to buffer: %s", err)
				}
			}
		}
	}
	return buf.Bytes(), nil
}

func writeIntoFile(baseDir string, fName string, blockBytes []byte) error {
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		log.Println("base directory %s does not exist", baseDir)
	}

	f, err := os.Create(filepath.Join(baseDir, fName))
	if err != nil {
		log.Println("error creating file: %s", err)
	}
	defer f.Close()

	if _, err := f.Write(blockBytes); err != nil {
		log.Println("error writing to file: %s", err)
	}

	return nil
}

func isEqual(A []string, B []string) bool {
	if len(A) != len(B) {
		return false
	} else {
		for i := 0; i < len(A); i++ {
			if A[i] != B[i] {
				return false
			}
		}
		return true
	}
}
func del_remotelyDeletedFiles(client RPCClient, filesRemotelyDeleted []string) {
	for _, eachFileName := range filesRemotelyDeleted {
		err := os.Remove(ConcatPath(client.BaseDir, eachFileName))
		if err != nil {
			log.Println("ERROR IN DELETING: ", eachFileName)
		}
	}
}

func isPresent(dataStructure []string, targetString string) bool {
	for _, element := range dataStructure {
		if element == targetString {
			return true
		}
	}
	return false
}

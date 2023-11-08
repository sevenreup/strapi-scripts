package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/lib/pq"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type FileFormats struct {
	URL       string `json:"url"`
	Large     Format `json:"large"`
	Small     Format `json:"small"`
	Medium    Format `json:"medium"`
	Thumbnail Format `json:"thumbnail"`
}

type Format struct {
	URL    string  `json:"url"`
	Ext    string  `json:"ext"`
	Hash   string  `json:"hash"`
	Mime   string  `json:"mime"`
	Name   string  `json:"name"`
	Path   string  `json:"path"`
	Size   float64 `json:"size"`
	Width  int     `json:"width"`
	Height int     `json:"height"`
}

func main() {
	var minioEndpoint, minioAccessKey, minioSecretKey, minioBucket, folderPath, pgConnectionString string

	flag.StringVar(&minioEndpoint, "minioEndpoint", "your-minio-server-url:9000", "Minio server endpoint")
	flag.StringVar(&minioAccessKey, "minioAccessKey", "your-access-key", "Minio access key")
	flag.StringVar(&minioSecretKey, "minioSecretKey", "your-secret-key", "Minio secret key")
	flag.StringVar(&minioBucket, "minioBucket", "your-minio-bucket", "Minio bucket name")
	flag.StringVar(&folderPath, "folderPath", "", "Path to the folder to upload")
	flag.StringVar(&pgConnectionString, "pgConnectionString", "postgresql://username:password@localhost/dbname?sslmode=disable", "PostgreSQL connection string")

	flag.Parse()

	minioClient, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalln(err)
	}

	pgDB, err := sql.Open("postgres", pgConnectionString)
	if err != nil {
		log.Fatalln(err)
	}
	defer pgDB.Close()

	updateStmt, err := pgDB.Prepare("UPDATE public.files SET url = $1, formats = $2 WHERE name = $3")
	if err != nil {
		log.Fatalln(err)
	}

	err = filepath.Walk(folderPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing %s: %v\n", filePath, err)
			return err
		}

		if info.IsDir() {
			return nil
		}

		objectName := strings.TrimPrefix(filePath, folderPath)
		filename := filepath.Base(filePath)

		var existingURL, existingFormats string
		err = pgDB.QueryRow("SELECT url, formats FROM public.files WHERE name = $1", filename).Scan(&existingURL, &existingFormats)

		if err != nil && err != sql.ErrNoRows {
			log.Printf("Error checking file existence in the database: %v\n", err)
		} else {
			if existingURL != "" {
				_, err = minioClient.FPutObject(context.Background(), minioBucket, objectName, filePath, minio.PutObjectOptions{})
				if err != nil {
					log.Printf("Error uploading %s: %v\n", filePath, err)
					return err
				}

				fmt.Printf("Successfully uploaded %s to %s\n", filePath, minioBucket)

				createUrl := func(path string) string {
					if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
						return path
					}
					path = strings.TrimPrefix(strings.ReplaceAll(path, "\\", "/"), "/")
					return fmt.Sprintf("https://%s/%s/%s", minioEndpoint, minioBucket, path)

				}

				var existingFormatsData FileFormats
				err := json.Unmarshal([]byte(existingFormats), &existingFormatsData)
				if err != nil {
					log.Printf("Error parsing existing formats JSON: %v\n", err)
				}

				existingFormatsData.URL = createUrl(objectName)
				existingFormatsData.Large.URL = createUrl(existingFormatsData.Large.URL)
				existingFormatsData.Small.URL = createUrl(existingFormatsData.Small.URL)
				existingFormatsData.Medium.URL = createUrl(existingFormatsData.Medium.URL)
				existingFormatsData.Thumbnail.URL = createUrl(existingFormatsData.Thumbnail.URL)

				updatedFormats, err := json.Marshal(existingFormatsData)
				if err != nil {
					log.Printf("Error converting updated formats to JSON: %v\n", err)
				}

				existingFormatsString := string(updatedFormats)

				_, err = updateStmt.Exec(existingFormatsData.URL, existingFormatsString, filename)
				if err != nil {
					log.Printf("Error updating the URL and formats in the database: %v\n", err)
				}
				fmt.Printf("Updated URL and formats for %s in the database\n", filename)
			} else {
				log.Printf("File %s does not exist in the database\n", filename)
			}
		}

		return nil
	})

	if err != nil {
		log.Fatalln(err)
	}
}

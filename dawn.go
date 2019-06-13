package dawn

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/minio/minio-go"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"time"
)

type MySQLConfig struct {
	UserName string
	Password string
	Host string
	Port int
	Database string
}

type AWSConfig struct {
	AccessKeyID string
	SecretAccessKey string
	Bucket string
}

type SelectQuery interface {
	Table() string
	Columns() []string
	Where() string
}

func Export(qs []SelectQuery, my *MySQLConfig, dest string) {
	dataSourceName := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s",
		my.UserName,
		my.Password,
		my.Host,
		my.Port,
		my.Database,
	)
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	for _, q := range(qs) {
		start := time.Now()
		stmt := "SELECT "
		for i, col := range(q.Columns()) {
			if i != 0 {
				stmt += ", "
			}
			stmt += col
		}
		stmt += " FROM "+q.Table()
		if q.Where() != "" {
			stmt += " WHERE "+q.Where()
		}
		fmt.Print("Executing ", "\""+stmt+"\" ")
		export(stmt, db, dest)
		end := time.Now()
		duration := int64(end.Sub(start) / time.Millisecond)
		fmt.Println(duration, "ms")
	}
}

func Upload(aws *AWSConfig, objectPrefix string, source string) {
	s3, err := minio.New(
		"s3.amazonaws.com",
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		true,
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	objectsCh := make(chan string)

	// Send object names that are needed to be removed to objectsCh
	go func() {
		defer close(objectsCh)

		doneCh := make(chan struct{})

		// Indicate to our routine to exit cleanly upon return.
		defer close(doneCh)

		// List all objects from a bucket-name with a matching prefix.
		for object := range s3.ListObjects(aws.Bucket, objectPrefix, true, doneCh) {
			if object.Err != nil {
				log.Fatalln(object.Err)
			}
			fmt.Println("Deleting "+object.Key)
			objectsCh <- object.Key
		}
	}()

	// Call RemoveObjects API
	errorCh := s3.RemoveObjects(aws.Bucket, objectsCh)

	// Print errors received from RemoveObjects API
	for e := range errorCh {
		log.Fatalln("Failed to delete " + e.ObjectName + ", error: " + e.Err.Error())
	}

	// Upload
	bucketName := aws.Bucket
	contentType := "text/csv"
	files, err := ioutil.ReadDir(source)
	if err != nil {
		fmt.Println(err)
	}
	for _, f := range files  {
		fmt.Printf("Uploading %s...", f.Name())
		objectName := objectPrefix+f.Name()
		filePath := source+"/"+f.Name()
		n, err := s3.FPutObject(bucketName, objectName, filePath, minio.PutObjectOptions{ContentType:contentType})
		fmt.Println(n)
		if err != nil {
			log.Fatalln(err)
		}

	}
}

func export(q string, db *sql.DB, dest string) {
	re := regexp.MustCompile("FROM `?([a-z_]+)`?")
	submatch := re.FindStringSubmatch(q)
	table := submatch[1]
	rows, err := db.Query(q)
	if err != nil {
		fmt.Println(err)
		return
	}
	if _, err = os.Stat(dest); os.IsNotExist(err) {
		os.Mkdir(dest, 0700)
	}
	err = output(rows,dest+"/"+table+".csv")
	if err != nil {
		fmt.Println(err)
	}
}

func output(rows *sql.Rows, fp string) error {
	f, err := os.Create(fp)
	if err != nil {
		return err
	}
	c := newConverter(rows)
	err = c.Write(f)
	if err != nil {
		f.Close() // close, but only return/handle the write error
		return err
	}

	return f.Close()
}

package qldb

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/awslabs/amazon-qldb-driver-go/v2/qldbdriver"
	"github.com/golang-migrate/migrate/v4/database"
	"io"
	"net/url"
)

func init() {
	db := QLDB{}
	database.Register("qldb", &db)
}

const DefaultMigrationsLabel = "SchemaMigration"

type QLDB struct {
	driver *qldbdriver.QLDBDriver
	config *Config
}

func WithInstance(instance *qldbdriver.QLDBDriver) (*QLDB, error) {
	return &QLDB{
		driver: instance,
	}, nil
}

// Open url: qldb://access_key_id:access_secret@region/database_name
func (Q QLDB) Open(dbUrl string) (database.Driver, error) {
	parsedUrl, err := url.Parse(dbUrl)
	if err != nil {
		return nil, err
	}

	secret, isSet := parsedUrl.User.Password()
	if !isSet {
		return nil, fmt.Errorf("secret is not set")
	}

	config := &Config{
		AccessKeyID:     parsedUrl.User.Username(),
		Region:          parsedUrl.Host,
		AccessKeySecret: secret,
		LedgerName:      parsedUrl.Path[1:],
	}

	awsSession, err := session.NewSession(&aws.Config{
		Region:      aws.String(config.Region),
		Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.AccessKeySecret, ""),
	})

	if err != nil {
		return nil, err
	}

	qldbSession := qldbsession.New(awsSession)

	driver, err := qldbdriver.New(
		config.LedgerName, qldbSession, func(options *qldbdriver.DriverOptions) {
			options.LoggerVerbosity = qldbdriver.LogInfo
		})
	if err != nil {
		return nil, err
	}

	return WithInstance(driver)
}

func (Q QLDB) Close() error {
	//TODO implement me
	fmt.Println("Close")
	panic("implement me")
}

func (Q QLDB) Lock() error {
	//TODO implement me
	fmt.Println("Lock")
	panic("implement me")
}

func (Q QLDB) Unlock() error {
	//TODO implement me
	fmt.Println("Unlock")
	panic("implement me")
}

func (Q QLDB) Run(migration io.Reader) error {
	//TODO implement me
	fmt.Println("Run")
	fmt.Println(migration)
	panic("implement me")
}

func (Q QLDB) SetVersion(version int, dirty bool) error {
	//TODO implement me
	fmt.Println("SetVersion")
	fmt.Println("version: ", version, "dirty: ", dirty)
	panic("implement me")
}

func (Q QLDB) Version() (version int, dirty bool, err error) {
	//TODO implement me
	fmt.Println("Version")
	fmt.Println("version: ", version, "dirty: ", dirty)
	panic("implement me")
}

func (Q QLDB) Drop() error {
	//TODO implement me
	fmt.Println("Drop")
	return nil
}

// Config ...
type Config struct {
	LedgerName      string
	Region          string
	AccessKeyID     string
	AccessKeySecret string
}

var (
	ErrNilConfig = fmt.Errorf("no config")
)

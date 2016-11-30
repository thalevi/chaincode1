package main

import (
    "fmt"
	"errors"
	"encoding/json"
	"github.com/hyperledger/fabric/core/chaincode/shim"
)

// setup a logger for the peers
var log = shim.NewLogger("att")

// AttestationChaincode is a Chaincode implementation of device attestation
type AttestationChaincode struct {
}

// initialize
func (t *AttestationChaincode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	log.Debug("Init called.")

	if len(args) != 0 {
		log.Debug("Init called with arguments.")
		return nil, errors.New("Init expects 0 arguments.")
	}

	log.Debug("Creating Quotes table...")
	// Create table for attestation quotes
	err := stub.CreateTable("Quotes", []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "hostname", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "userid", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "timestamp", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "nonce", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "quote", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "eventlog", Type: shim.ColumnDefinition_STRING, Key: false},
	})

	if err != nil {
		return nil, errors.New("Failed to create Quotes table.")
	}

	log.Debug("Successfully created Quotes table.")


	log.Debug("Creating Attestation table...")
	// Create table for attestation validation
	// TODO: stop calling the prev_transaction_uuid a nonce?
	err2 := stub.CreateTable("Attestation", []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "client_hostname", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "validator_hostname", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "timestamp", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "quote_timestamp", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "quote_nonce", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "is_valid", Type: shim.ColumnDefinition_STRING, Key: false},
	})

	if err2 != nil {
		return nil, errors.New("Failed to create Attestation table.")
	}

	log.Debug("Successfully created Attestation table.")

	return nil, nil
}

// used to refresh tables to start from the beginning
func (t *AttestationChaincode) refreshAttTables(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
        log.Debug("Deleting Attestation Tables...")

        err := stub.DeleteTable("Quotes")
        if err != nil {
                log.Debug("Error deleting table. [", err, "]")
        }

        err = stub.DeleteTable("Attestation")
        if err != nil {
                log.Debug("Error deleting table. [", err, "]")
        }

        log.Debug("Creating Quotes table...")
        // Create table for attestation quotes
        err = stub.CreateTable("Quotes", []*shim.ColumnDefinition{
                &shim.ColumnDefinition{Name: "hostname", Type: shim.ColumnDefinition_STRING, Key: true},
                &shim.ColumnDefinition{Name: "userid", Type: shim.ColumnDefinition_STRING, Key: true},
                &shim.ColumnDefinition{Name: "timestamp", Type: shim.ColumnDefinition_STRING, Key: true},
                &shim.ColumnDefinition{Name: "nonce", Type: shim.ColumnDefinition_STRING, Key: true},
                &shim.ColumnDefinition{Name: "quote", Type: shim.ColumnDefinition_STRING, Key: false},
                &shim.ColumnDefinition{Name: "eventlog", Type: shim.ColumnDefinition_STRING, Key: false},
        })

        if err != nil {
                return nil, errors.New("Failed to create Quotes table.")
        }

        log.Debug("Successfully created Quotes table.")


        log.Debug("Creating Attestation table...")
        // Create table for attestation validation
        // TODO: stop calling the prev_transaction_uuid a nonce?
	err = stub.CreateTable("Attestation", []*shim.ColumnDefinition{
                &shim.ColumnDefinition{Name: "client_hostname", Type: shim.ColumnDefinition_STRING, Key: true},
                &shim.ColumnDefinition{Name: "validator_hostname", Type: shim.ColumnDefinition_STRING, Key: true},
                &shim.ColumnDefinition{Name: "timestamp", Type: shim.ColumnDefinition_STRING, Key: true},
                &shim.ColumnDefinition{Name: "quote_timestamp", Type: shim.ColumnDefinition_STRING, Key: true},
                &shim.ColumnDefinition{Name: "quote_nonce", Type: shim.ColumnDefinition_STRING, Key: true},
                &shim.ColumnDefinition{Name: "is_valid", Type: shim.ColumnDefinition_STRING, Key: false},
        })

        if err != nil {
                return nil, errors.New("Failed to create Attestation table.")
        }

        return nil, nil
}

// do stuff
func (t *AttestationChaincode) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	log.Debug("Invoke called.")

	// Handle different functions
	if function == "quote" {
		return t.quote(stub, args)
	} else if function == "attest" {
		return t.attest(stub, args)
	} else if function == "refreshatttables" {
                return t.refreshAttTables(stub, args)
        }

	return nil, errors.New("Invalid function invoked.")
}

// quote to the ledger
func (t *AttestationChaincode) quote(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	log.Debugf("Quote called with args: {%v}.", args)

	if len(args) != 6 {
		return nil, errors.New("Quote expects 6 arguments.")
	}

	hostname, userid, timestamp, nonce, quote, eventlog := args[0], args[1], args[2], args[3], args[4], args[5]

	ok, err := stub.InsertRow("Quotes", shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: hostname}},
			&shim.Column{Value: &shim.Column_String_{String_: userid}},
			&shim.Column{Value: &shim.Column_String_{String_: timestamp}},
			&shim.Column{Value: &shim.Column_String_{String_: nonce}},
			&shim.Column{Value: &shim.Column_String_{String_: quote}},
			&shim.Column{Value: &shim.Column_String_{String_: eventlog}},
		},
	})

	if !ok && err == nil {
		return nil, errors.New("Quote already exists.")
	}

	return nil, nil
}

// attest to the ledger
func (t *AttestationChaincode) attest(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	log.Debugf("Attest called with args: {%v}.", args)

	if len(args) != 6 {
		return nil, errors.New("Attest expects 6 arguments.")
	}

	client_hostname, validator_hostname, timestamp, quote_timestamp, quote_nonce, is_valid := args[0], args[1], args[2], args[3], args[4], args[5]

	ok, err := stub.InsertRow("Attestation", shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: client_hostname}},
			&shim.Column{Value: &shim.Column_String_{String_: validator_hostname}},
			&shim.Column{Value: &shim.Column_String_{String_: timestamp}},
			&shim.Column{Value: &shim.Column_String_{String_: quote_timestamp}},
			&shim.Column{Value: &shim.Column_String_{String_: quote_nonce}},
			&shim.Column{Value: &shim.Column_String_{String_: is_valid}},
		},
	})

	if !ok && err == nil {
		return nil, errors.New("Attestation already exists.")
	}

	return nil, nil
}

// query world state
func (t *AttestationChaincode) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	log.Debug("Query called.")

	// Handle different functions
	if function == "quotes_by_hostname" {
		return t.query_quotes_by_hostname(stub, args)
	} else if function == "client_attested_by" {
		return t.query_client_attested_by(stub, args)
	} else if function == "attestations_by_client" {
		return t.query_attestations_by_client(stub, args)
	}

	return nil, errors.New("Invalid function invoked.")
}


func (t *AttestationChaincode) query_quotes_by_hostname(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	log.Debugf("Query_by_hostname called with args: {%v}.", args)

	if len(args) != 1 {
		return nil, errors.New("Query_by_hostname expects 1 argument.")
	}

	hostname := args[0]

	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: hostname}}
	columns = append(columns, col1)

	rowChannel, err := stub.GetRows("Quotes", columns)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. %s", err)
	}

	var rows []shim.Row
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)
			}
		}
		if rowChannel == nil {
			break
		}
	}

	jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return jsonRows, nil
}

func (t *AttestationChaincode) query_client_attested_by(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	log.Debugf("Query_client_attested_by called with args: {%v}.", args)

	if len(args) != 2 {
		return nil, errors.New("Query_client_attested_by expects 2 argument.")
	}

	client_hostname := args[0]
	validator_hostname := args[1]

	//TODO: how to search by column, and not just partial key?
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: client_hostname}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: validator_hostname}}
	columns = append(columns, col1, col2)

	rowChannel, err := stub.GetRows("Attestation", columns)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. %s", err)
	}

	var rows []shim.Row
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)
			}
		}
		if rowChannel == nil {
			break
		}
	}

	fmt.Println("No of records = ", len(rows))
	jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return jsonRows, nil
}

func (t *AttestationChaincode) query_attestations_by_client(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	log.Debugf("Query_attestations_by_client called with args: {%v}.", args)

	if len(args) != 1 {
		return nil, errors.New("Query_attestations_by_client expects 1 argument.")
	}

	client_hostname := args[0]

	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: client_hostname}}
	columns = append(columns, col1)

	rowChannel, err := stub.GetRows("Attestation", columns)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. %s", err)
	}

	var rows []shim.Row
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)
			}
		}
		if rowChannel == nil {
			break
		}
	}

	jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return jsonRows, nil
}


func main() {
	log.SetLevel(shim.LogDebug)

    err := shim.Start(new(AttestationChaincode))
    if err != nil {
        fmt.Printf("Error starting Simple chaincode: %s", err)
    }
}

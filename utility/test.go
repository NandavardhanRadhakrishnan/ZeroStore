package test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

type Row struct {
	TextField    string
	NumericField float64
	Timestamp    time.Time
	UUID         string
}

func randomText(length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	rand.Seed(time.Now().UnixNano())
	text := make([]rune, length)
	for i := range text {
		text[i] = letters[rand.Intn(len(letters))]
	}
	return string(text)
}

func randomTimestamp() time.Time {
	min := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

func randomUUID() string {
	uuid, err := uuid.NewRandom()
	if err != nil {
		fmt.Println("Error generating UUID:", err)
		return ""
	}
	return uuid.String()
}

func GenerateRow(textLength int) Row {
	return Row{
		TextField:    randomText(textLength),
		NumericField: rand.Float64(),
		Timestamp:    randomTimestamp(),
		UUID:         randomUUID(),
	}
}

const NumberOfRows = 1000000

func main() {
	// Define row schema
	textLength := 1024 // 1 KB of text
	numericField := rand.Float64()
	timestamp := randomTimestamp()
	uuid := randomUUID()

	// Generate random row data
	randomRow := struct {
		TextField    string
		NumericField float64
		Timestamp    time.Time
		UUID         string
	}{
		TextField:    randomText(textLength),
		NumericField: numericField,
		Timestamp:    timestamp,
		UUID:         uuid,
	}

	// Calculate size of each field
	textFieldSize := len(randomRow.TextField)
	numericFieldSize := binary.Size(randomRow.NumericField)
	timestampSize := binary.Size(randomRow.Timestamp)
	uuidSize := len(randomRow.UUID)

	// Calculate total size of a single row
	totalRowSize := textFieldSize + numericFieldSize + timestampSize + uuidSize

	// Define number of rows
	NumberOfRows := 1000000 // 1 million rows

	// Calculate total file size
	totalFileSize := int64(totalRowSize) * int64(NumberOfRows)

	// Output the results
	fmt.Printf("Text Field Size: %d bytes\n", textFieldSize)
	fmt.Printf("Numeric Field Size: %d bytes\n", numericFieldSize)
	fmt.Printf("Timestamp Size: %d bytes\n", timestampSize)
	fmt.Printf("UUID Size: %d bytes\n", uuidSize)
	fmt.Printf("Total Row Size: %d bytes\n", totalRowSize)
	fmt.Printf("Total File Size for %d rows: %d bytes\n", NumberOfRows, totalFileSize)
}

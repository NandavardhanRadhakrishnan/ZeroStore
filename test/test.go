package test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
)

type Row struct {
	TextField    string
	NumericField float64
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
		UUID:         randomUUID(),
	}
}

func getFileSize(filePath string) (int64, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

func CalculateEfficiencyPercentage(dataFile string, numRows int, textLength int) error {
	dataSize, err := getFileSize(dataFile)
	if err != nil {
		return err
	}

	// Ideal size calculation
	textFieldSize := textLength
	numericFieldSize := 8 // size of float64
	uuidSize := 36        // UUID length
	totalRowSize := textFieldSize + numericFieldSize + uuidSize
	totalFileSize := int64(totalRowSize) * int64(numRows)

	// Calculate efficiency percentage
	efficiency := (float64(totalFileSize) / float64(dataSize)) * 100

	// Output the results
	fmt.Printf("Text Field Size: %d bytes\n", textFieldSize)
	fmt.Printf("Numeric Field Size: %d bytes\n", numericFieldSize)
	fmt.Printf("UUID Size: %d bytes\n", uuidSize)
	fmt.Printf("Total Row Size: %d bytes\n", totalRowSize)
	fmt.Printf("Expected Total File Size for %d rows: %d bytes\n", numRows, totalFileSize)
	fmt.Printf("Actual Data File Size: %d bytes\n", dataSize)
	fmt.Printf("Efficiency Percentage: %.2f%%\n", efficiency)

	return nil
}

const NumberOfRows = 1000000

func Disp() {
	// Define row schema
	textLength := 1024 // 1 KB of text
	numericField := rand.Float64()
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
		UUID:         uuid,
	}

	// Calculate size of each field
	textFieldSize := len(randomRow.TextField)
	numericFieldSize := binary.Size(randomRow.NumericField)
	uuidSize := len(randomRow.UUID)

	// Calculate total size of a single row
	totalRowSize := textFieldSize + numericFieldSize + uuidSize

	// Define number of rows
	NumberOfRows := 1000000 // 1 million rows

	// Calculate total file size
	totalFileSize := int64(totalRowSize) * int64(NumberOfRows)

	// Output the results
	fmt.Printf("Text Field Size: %d bytes\n", textFieldSize)
	fmt.Printf("Numeric Field Size: %d bytes\n", numericFieldSize)
	fmt.Printf("UUID Size: %d bytes\n", uuidSize)
	fmt.Printf("Total Row Size: %d bytes\n", totalRowSize)
	fmt.Printf("Total File Size for %d rows: %d bytes\n", NumberOfRows, totalFileSize)
}

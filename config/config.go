package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	TelegramToken    string
	SpreadsheetID    string
	CredentialsPath  string
	SyncIntervalHours int
}

var AppConfig *Config

func Load() error {
	// Загружаем .env файл, если он существует
	if err := godotenv.Load(); err != nil {
		log.Printf("Предупреждение: .env файл не найден, используем переменные окружения")
	}

	AppConfig = &Config{
		TelegramToken:     getEnv("TELEGRAM_BOT_TOKEN", ""),
		SpreadsheetID:     getEnv("SPREADSHEET_ID", ""),
		CredentialsPath:   getEnv("GOOGLE_CREDENTIALS_PATH", "credentials.json"),
		SyncIntervalHours: getEnvInt("SYNC_INTERVAL_HOURS", 2),
	}

	if AppConfig.TelegramToken == "" {
		return &ConfigError{Message: "TELEGRAM_BOT_TOKEN не установлен"}
	}

	if AppConfig.SpreadsheetID == "" {
		return &ConfigError{Message: "SPREADSHEET_ID не установлен"}
	}

	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	
	var result int
	if _, err := fmt.Sscanf(value, "%d", &result); err != nil {
		log.Printf("Ошибка парсинга %s, используем значение по умолчанию: %d", key, defaultValue)
		return defaultValue
	}
	return result
}

type ConfigError struct {
	Message string
}

func (e *ConfigError) Error() string {
	return e.Message
}

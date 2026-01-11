package main

import (
	"log"
	"os"

	"ss_ref_bot/bot"
	"ss_ref_bot/config"
	"ss_ref_bot/sheets"
)

func main() {
	// Загружаем конфигурацию
	if err := config.Load(); err != nil {
		log.Fatalf("Ошибка загрузки конфигурации: %v", err)
	}

	// Создаем клиент Google Sheets
	sheetsClient, err := sheets.NewSheetsClient(
		config.AppConfig.SpreadsheetID,
		config.AppConfig.CredentialsPath,
	)
	if err != nil {
		log.Fatalf("Ошибка создания клиента Google Sheets: %v", err)
	}

	// Проверяем наличие файла credentials
	if _, err := os.Stat(config.AppConfig.CredentialsPath); os.IsNotExist(err) {
		log.Fatalf("Файл credentials не найден: %s", config.AppConfig.CredentialsPath)
	}

	// Создаем бота
	telegramBot, err := bot.NewBot(config.AppConfig.TelegramToken, sheetsClient)
	if err != nil {
		log.Fatalf("Ошибка создания бота: %v", err)
	}

	log.Println("Бот запущен и готов к работе...")

	// Запускаем бота (блокирующий вызов)
	if err := telegramBot.Start(); err != nil {
		log.Fatalf("Ошибка запуска бота: %v", err)
	}
}

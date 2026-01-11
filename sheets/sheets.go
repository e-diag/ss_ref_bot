package sheets

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

type SheetsClient struct {
	service       *sheets.Service
	spreadsheetID string

	// Кэш для быстрого поиска
	cacheMutex      sync.RWMutex
	referrersByID   map[int64]*Referrer
	referrersByCode map[string]*Referrer // нормализованный код -> Referrer
	invitedByUserID map[int64]*Invited
	existingDealIDs map[string]bool
	lastCacheUpdate time.Time
}

type Referrer struct {
	ID            int64
	Username      string
	Code          string
	Wallet        string
	RefCount      int
	PendingPayout float64
	PaidOut       float64 // Выплачено (колонка G)
}

type Invited struct {
	UserID  int64
	RefCode string
}

type Referral struct {
	RefID   int64
	RefCode string
	Profit  float64
	DealID  string
	Bonus   float64
	Date    string
}

type Withdrawal struct {
	DealID string
	UserID int64
	Profit float64
}

func NewSheetsClient(spreadsheetID, credentialsPath string) (*SheetsClient, error) {
	ctx := context.Background()

	service, err := sheets.NewService(ctx, option.WithCredentialsFile(credentialsPath))
	if err != nil {
		return nil, fmt.Errorf("ошибка создания клиента Google Sheets: %w", err)
	}

	client := &SheetsClient{
		service:         service,
		spreadsheetID:   spreadsheetID,
		referrersByID:   make(map[int64]*Referrer),
		referrersByCode: make(map[string]*Referrer),
		invitedByUserID: make(map[int64]*Invited),
		existingDealIDs: make(map[string]bool),
	}

	// Загружаем кэш при инициализации
	if err := client.LoadCache(); err != nil {
		log.Printf("Предупреждение: не удалось загрузить кэш при инициализации: %v", err)
	}

	return client, nil
}

// LoadCache загружает все данные в кэш для быстрого поиска
func (sc *SheetsClient) LoadCache() error {
	sc.cacheMutex.Lock()
	defer sc.cacheMutex.Unlock()

	log.Printf("Загрузка кэша...")

	// Загружаем рефоводов
	if err := sc.loadReferrersCache(); err != nil {
		return fmt.Errorf("ошибка загрузки кэша рефоводов: %w", err)
	}

	// Загружаем приглашенных
	if err := sc.loadInvitedCache(); err != nil {
		return fmt.Errorf("ошибка загрузки кэша приглашенных: %w", err)
	}

	// Загружаем существующие DealIDs
	if err := sc.loadDealIDsCache(); err != nil {
		return fmt.Errorf("ошибка загрузки кэша DealIDs: %w", err)
	}

	sc.lastCacheUpdate = time.Now()
	log.Printf("Кэш загружен: рефоводов=%d, приглашенных=%d, сделок=%d",
		len(sc.referrersByID), len(sc.invitedByUserID), len(sc.existingDealIDs))

	return nil
}

// loadReferrersCache загружает рефоводов в кэш
func (sc *SheetsClient) loadReferrersCache() error {
	readRange := "Рефоводы!A2:G"
	resp, err := sc.service.Spreadsheets.Values.Get(sc.spreadsheetID, readRange).
		ValueRenderOption("UNFORMATTED_VALUE").Do()
	if err != nil {
		return fmt.Errorf("ошибка чтения листа Рефоводы: %w", err)
	}

	sc.referrersByID = make(map[int64]*Referrer)
	sc.referrersByCode = make(map[string]*Referrer)

	if resp.Values == nil {
		return nil
	}

	for _, row := range resp.Values {
		if len(row) < 1 {
			continue
		}

		ref := sc.parseReferrerRow(row)
		if ref == nil {
			continue
		}

		// Добавляем в кэш по ID
		sc.referrersByID[ref.ID] = ref

		// Добавляем в кэш по коду (нормализованному)
		if ref.Code != "" {
			normalizedCode := strings.ToUpper(strings.TrimSpace(ref.Code))
			sc.referrersByCode[normalizedCode] = ref
		}
	}

	return nil
}

// parseReferrerRow парсит строку рефовода из таблицы
func (sc *SheetsClient) parseReferrerRow(row []interface{}) *Referrer {
	if len(row) < 1 {
		return nil
	}

	// Пробуем получить ID разными способами
	var id int64
	switch v := row[0].(type) {
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil
		}
		id = parsed
	case int64:
		id = v
	case int:
		id = int64(v)
	case float64:
		id = int64(v)
	default:
		idStr := getStringValue(row[0])
		if idStr == "" {
			return nil
		}
		parsed, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			return nil
		}
		id = parsed
	}

	ref := &Referrer{ID: id}

	if len(row) > 1 {
		ref.Username = getStringValue(row[1])
	}
	if len(row) > 2 {
		ref.Code = getStringValue(row[2])
	}
	if len(row) > 3 {
		ref.Wallet = getStringValue(row[3])
	}
	if len(row) > 4 {
		ref.RefCount = getIntValue(row[4])
	}
	if len(row) > 5 {
		ref.PendingPayout = getFloatValue(row[5])
	}
	if len(row) > 6 {
		ref.PaidOut = getFloatValue(row[6])
	}

	return ref
}

// loadInvitedCache загружает приглашенных в кэш
func (sc *SheetsClient) loadInvitedCache() error {
	readRange := "Приглашенные!A2:B"
	resp, err := sc.service.Spreadsheets.Values.Get(sc.spreadsheetID, readRange).Do()
	if err != nil {
		return fmt.Errorf("ошибка чтения листа Приглашенные: %w", err)
	}

	sc.invitedByUserID = make(map[int64]*Invited)

	if resp.Values == nil {
		return nil
	}

	for _, row := range resp.Values {
		if len(row) < 2 {
			continue
		}

		var userID int64
		switch v := row[0].(type) {
		case string:
			parsed, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				continue
			}
			userID = parsed
		case int64:
			userID = v
		case int:
			userID = int64(v)
		case float64:
			userID = int64(v)
		default:
			continue
		}

		invited := &Invited{
			UserID:  userID,
			RefCode: getStringValue(row[1]),
		}

		sc.invitedByUserID[userID] = invited
	}

	return nil
}

// loadDealIDsCache загружает существующие DealIDs в кэш
func (sc *SheetsClient) loadDealIDsCache() error {
	readRange := "Рефералы!D2:D"
	resp, err := sc.service.Spreadsheets.Values.Get(sc.spreadsheetID, readRange).Do()
	if err != nil {
		return fmt.Errorf("ошибка чтения листа Рефералы: %w", err)
	}

	sc.existingDealIDs = make(map[string]bool)

	if resp.Values == nil {
		return nil
	}

	for _, row := range resp.Values {
		if len(row) > 0 {
			dealID := getStringValue(row[0])
			if dealID != "" {
				sc.existingDealIDs[dealID] = true
			}
		}
	}

	return nil
}

// GetReferrerByID получает рефовода по ID из кэша
func (sc *SheetsClient) GetReferrerByID(userID int64) (*Referrer, error) {
	sc.cacheMutex.RLock()
	defer sc.cacheMutex.RUnlock()

	ref, exists := sc.referrersByID[userID]
	if !exists {
		return nil, nil
	}

	// Возвращаем копию, чтобы избежать гонок данных
	refCopy := *ref
	return &refCopy, nil
}

// findFirstEmptyRow находит первую пустую строку в листе (начиная со строки 2)
func (sc *SheetsClient) findFirstEmptyRow(sheetName string) (int, error) {
	readRange := fmt.Sprintf("%s!A2:A", sheetName)
	resp, err := sc.service.Spreadsheets.Values.Get(sc.spreadsheetID, readRange).Do()
	if err != nil {
		return 2, fmt.Errorf("ошибка чтения листа %s: %w", sheetName, err)
	}

	if len(resp.Values) == 0 {
		return 2, nil // Первая строка после заголовка
	}

	// Ищем первую пустую строку
	for i, row := range resp.Values {
		if len(row) == 0 || getStringValue(row[0]) == "" {
			return i + 2, nil // +2 потому что начинаем с строки 2 и индексация с 0
		}
	}

	// Если все строки заполнены, возвращаем следующую после последней
	return len(resp.Values) + 2, nil
}

// CreateReferrer создает нового рефовода
func (sc *SheetsClient) CreateReferrer(userID int64, username string) (*Referrer, error) {
	// Генерируем уникальный код
	code, err := sc.generateUniqueCode()
	if err != nil {
		return nil, fmt.Errorf("ошибка генерации кода: %w", err)
	}

	ref := &Referrer{
		ID:            userID,
		Username:      username,
		Code:          code,
		RefCount:      0,
		PendingPayout: 0.0,
		PaidOut:       0.0,
	}

	// Находим первую пустую строку
	rowIndex, err := sc.findFirstEmptyRow("Рефоводы")
	if err != nil {
		return nil, fmt.Errorf("ошибка поиска пустой строки: %w", err)
	}

	// Важно: пустые значения должны быть пустыми строками, а не nil
	walletValue := ""
	if ref.Wallet != "" {
		walletValue = ref.Wallet
	}

	values := [][]interface{}{
		{
			fmt.Sprintf("%d", ref.ID), // Колонка A: ID
			ref.Username,              // Колонка B: Username
			ref.Code,                  // Колонка C: Код
			walletValue,               // Колонка D: Кошелёк (может быть пустым)
			ref.RefCount,              // Колонка E: Количество рефералов
			ref.PendingPayout,         // Колонка F: Ожидает выплаты
			ref.PaidOut,               // Колонка G: Выплачено
		},
	}

	log.Printf("📝 Запись в Рефоводы (строка %d): ID=%d, Username=%s, Code=%s, Wallet=%s, RefCount=%d, PendingPayout=%.2f, PaidOut=%.2f",
		rowIndex, ref.ID, ref.Username, ref.Code, walletValue, ref.RefCount, ref.PendingPayout, ref.PaidOut)

	valueRange := &sheets.ValueRange{
		Values: values,
	}

	// Используем Update с конкретной строкой вместо Append
	updateRange := fmt.Sprintf("Рефоводы!A%d:G%d", rowIndex, rowIndex)
	updateResp, err := sc.service.Spreadsheets.Values.Update(
		sc.spreadsheetID,
		updateRange,
		valueRange,
	).ValueInputOption("USER_ENTERED").Do()

	if err != nil {
		log.Printf("❌ Ошибка записи в Рефоводы: %v", err)
		return nil, fmt.Errorf("ошибка добавления рефовода: %w", err)
	}

	log.Printf("✅ Рефовод успешно создан: ID=%d, код=%s, username=%s (строка %d)", ref.ID, ref.Code, ref.Username, rowIndex)
	if updateResp.UpdatedCells > 0 {
		log.Printf("   Обновлено ячеек: %d, диапазон: %s", updateResp.UpdatedCells, updateResp.UpdatedRange)
	}

	return ref, nil
}

// UpdateReferrer обновляет данные рефовода
func (sc *SheetsClient) UpdateReferrer(ref *Referrer) error {
	readRange := "Рефоводы!A2:G"
	resp, err := sc.service.Spreadsheets.Values.Get(sc.spreadsheetID, readRange).Do()
	if err != nil {
		return fmt.Errorf("ошибка чтения листа Рефоводы: %w", err)
	}

	if resp.Values == nil {
		return fmt.Errorf("рефовод не найден")
	}

	rowIndex := -1
	for i, row := range resp.Values {
		if len(row) < 1 {
			continue
		}

		idStr, ok := row[0].(string)
		if !ok {
			continue
		}

		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			continue
		}

		if id == ref.ID {
			rowIndex = i + 2 // +2 потому что первая строка - заголовок, и индексация с 1
			break
		}
	}

	if rowIndex == -1 {
		return fmt.Errorf("рефовод не найден")
	}

	// Обновляем строку
	updateRange := fmt.Sprintf("Рефоводы!A%d:G%d", rowIndex, rowIndex)

	// Важно: пустые значения должны быть пустыми строками
	walletValue := ""
	if ref.Wallet != "" {
		walletValue = ref.Wallet
	}

	values := [][]interface{}{
		{
			fmt.Sprintf("%d", ref.ID), // Колонка A: ID
			ref.Username,              // Колонка B: Username
			ref.Code,                  // Колонка C: Код
			walletValue,               // Колонка D: Кошелёк
			ref.RefCount,              // Колонка E: Количество рефералов
			ref.PendingPayout,         // Колонка F: Ожидает выплаты
			ref.PaidOut,               // Колонка G: Выплачено
		},
	}

	log.Printf("📝 Обновление Рефоводы (строка %d): ID=%d, Username=%s, Code=%s, Wallet=%s, RefCount=%d, PendingPayout=%.2f",
		rowIndex, ref.ID, ref.Username, ref.Code, walletValue, ref.RefCount, ref.PendingPayout)

	valueRange := &sheets.ValueRange{
		Values: values,
	}

	updateResp, err := sc.service.Spreadsheets.Values.Update(
		sc.spreadsheetID,
		updateRange,
		valueRange,
	).ValueInputOption("USER_ENTERED").Do()

	if err != nil {
		log.Printf("❌ Ошибка обновления Рефоводы: %v", err)
		return fmt.Errorf("ошибка обновления рефовода: %w", err)
	}

	log.Printf("✅ Рефовод обновлен: ID=%d, кошелек=%s, рефералов=%d, ожидает=%.2f", ref.ID, ref.Wallet, ref.RefCount, ref.PendingPayout)
	if updateResp.UpdatedCells > 0 {
		log.Printf("   Обновлено ячеек: %d, диапазон: %s", updateResp.UpdatedCells, updateResp.UpdatedRange)
	} else {
		log.Printf("   ⚠️ Обновлено ячеек: 0")
	}

	// Обновляем кэш
	sc.cacheMutex.Lock()
	sc.referrersByID[ref.ID] = ref
	if ref.Code != "" {
		normalizedCode := strings.ToUpper(strings.TrimSpace(ref.Code))
		sc.referrersByCode[normalizedCode] = ref
	}
	sc.cacheMutex.Unlock()

	return nil
}

// generateUniqueCode генерирует уникальный 6-символьный код
func (sc *SheetsClient) generateUniqueCode() (string, error) {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const codeLength = 6

	maxAttempts := 100
	charsetLen := big.NewInt(int64(len(charset)))

	for i := 0; i < maxAttempts; i++ {
		code := make([]byte, codeLength)
		for j := range code {
			// Используем crypto/rand для криптографически стойкой генерации
			n, err := rand.Int(rand.Reader, charsetLen)
			if err != nil {
				return "", fmt.Errorf("ошибка генерации случайного числа: %w", err)
			}
			code[j] = charset[n.Int64()]
		}

		codeStr := string(code)

		// Проверяем уникальность
		exists, err := sc.codeExists(codeStr)
		if err != nil {
			return "", err
		}

		if !exists {
			return codeStr, nil
		}

		// Небольшая задержка перед следующей попыткой
		time.Sleep(10 * time.Millisecond)
	}

	return "", fmt.Errorf("не удалось сгенерировать уникальный код после %d попыток", maxAttempts)
}

// codeExists проверяет существование кода
func (sc *SheetsClient) codeExists(code string) (bool, error) {
	readRange := "Рефоводы!C2:C"
	resp, err := sc.service.Spreadsheets.Values.Get(sc.spreadsheetID, readRange).Do()
	if err != nil {
		return false, err
	}

	if resp.Values == nil {
		return false, nil
	}

	for _, row := range resp.Values {
		if len(row) > 0 {
			if getStringValue(row[0]) == code {
				return true, nil
			}
		}
	}

	return false, nil
}

// GetInvitedByUserID получает запись из Приглашенные по ID пользователя из кэша
func (sc *SheetsClient) GetInvitedByUserID(userID int64) (*Invited, error) {
	sc.cacheMutex.RLock()
	defer sc.cacheMutex.RUnlock()

	invited, exists := sc.invitedByUserID[userID]
	if !exists {
		return nil, nil
	}

	// Возвращаем копию
	invitedCopy := *invited
	return &invitedCopy, nil
}

// CreateInvited создает запись в Приглашенные
func (sc *SheetsClient) CreateInvited(userID int64, refCode string) error {
	// Находим первую пустую строку
	rowIndex, err := sc.findFirstEmptyRow("Приглашенные")
	if err != nil {
		return fmt.Errorf("ошибка поиска пустой строки: %w", err)
	}

	values := [][]interface{}{
		{
			fmt.Sprintf("%d", userID), // Колонка A: ID пользователя
			refCode,                   // Колонка B: Код пригласившего
		},
	}

	log.Printf("📝 Запись в Приглашенные (строка %d): UserID=%d, код=%s", rowIndex, userID, refCode)

	valueRange := &sheets.ValueRange{
		Values: values,
	}

	// Используем Update с конкретной строкой вместо Append
	updateRange := fmt.Sprintf("Приглашенные!A%d:B%d", rowIndex, rowIndex)
	updateResp, err := sc.service.Spreadsheets.Values.Update(
		sc.spreadsheetID,
		updateRange,
		valueRange,
	).ValueInputOption("USER_ENTERED").Do()

	if err != nil {
		log.Printf("❌ Ошибка записи в Приглашенные: %v", err)
		return fmt.Errorf("ошибка добавления в Приглашенные: %w", err)
	}

	log.Printf("✅ Добавлен в Приглашенные: UserID=%d, код=%s (строка %d)", userID, refCode, rowIndex)
	if updateResp.UpdatedCells > 0 {
		log.Printf("   Обновлено ячеек: %d, диапазон: %s", updateResp.UpdatedCells, updateResp.UpdatedRange)
	}

	// Обновляем кэш
	sc.cacheMutex.Lock()
	sc.invitedByUserID[userID] = &Invited{UserID: userID, RefCode: refCode}
	sc.cacheMutex.Unlock()

	return nil
}

// GetReferrerByCode получает рефовода по коду из кэша
func (sc *SheetsClient) GetReferrerByCode(code string) (*Referrer, error) {
	sc.cacheMutex.RLock()
	defer sc.cacheMutex.RUnlock()

	// Нормализуем код
	normalizedCode := strings.ToUpper(strings.TrimSpace(code))

	ref, exists := sc.referrersByCode[normalizedCode]
	if !exists {
		return nil, nil
	}

	// Возвращаем копию, чтобы избежать гонок данных
	refCopy := *ref
	return &refCopy, nil
}

// IncrementRefCount увеличивает счетчик рефералов
func (sc *SheetsClient) IncrementRefCount(refCode string) error {
	ref, err := sc.GetReferrerByCode(refCode)
	if err != nil {
		return err
	}

	if ref == nil {
		return fmt.Errorf("рефовод с кодом %s не найден", refCode)
	}

	ref.RefCount++
	log.Printf("Увеличение счетчика рефералов для кода %s: теперь %d", refCode, ref.RefCount)
	return sc.UpdateReferrer(ref)
}

// GetExistingDealIDs получает список всех ID сделок из кэша
func (sc *SheetsClient) GetExistingDealIDs() (map[string]bool, error) {
	sc.cacheMutex.RLock()
	defer sc.cacheMutex.RUnlock()

	// Возвращаем копию map
	dealIDs := make(map[string]bool, len(sc.existingDealIDs))
	for k, v := range sc.existingDealIDs {
		dealIDs[k] = v
	}

	return dealIDs, nil
}

// GetNewWithdrawals получает новые выводы (которых еще нет в Рефералы)
func (sc *SheetsClient) GetNewWithdrawals() ([]Withdrawal, error) {
	existingDealIDs, err := sc.GetExistingDealIDs()
	if err != nil {
		return nil, err
	}

	readRange := "Выводы!A2:D"
	// Используем UNFORMATTED_VALUE для получения вычисленных значений из IMPORTRANGE
	resp, err := sc.service.Spreadsheets.Values.Get(sc.spreadsheetID, readRange).
		ValueRenderOption("UNFORMATTED_VALUE").Do()
	if err != nil {
		return nil, fmt.Errorf("ошибка чтения листа Выводы: %w", err)
	}

	if resp.Values == nil {
		return []Withdrawal{}, nil
	}

	var withdrawals []Withdrawal
	for _, row := range resp.Values {
		// Проверяем минимальное количество колонок: A (DealID), B (UserID), D (Profit)
		// Колонка C может отсутствовать из-за IMPORTRANGE, поэтому проверяем len >= 4
		// но Profit может быть в индексе 3 (если C есть) или 2 (если C отсутствует)
		if len(row) < 2 {
			continue
		}

		dealID := getStringValue(row[0])
		if dealID == "" {
			continue
		}

		// Пропускаем уже обработанные сделки
		if existingDealIDs[dealID] {
			continue
		}

		// Пробуем получить UserID разными способами
		var userID int64
		if len(row) < 2 {
			log.Printf("Пропуск сделки %s: недостаточно колонок для UserID", dealID)
			continue
		}

		switch v := row[1].(type) {
		case string:
			// Убираем неразрывные пробелы и другие пробельные символы
			cleaned := strings.ReplaceAll(v, "\u00a0", "") // неразрывный пробел
			cleaned = strings.ReplaceAll(cleaned, " ", "")
			cleaned = strings.TrimSpace(cleaned)

			// Пропускаем если это текст (не число)
			if cleaned == "" || strings.HasPrefix(strings.ToLower(cleaned), "без") {
				log.Printf("Пропуск сделки %s: UserID содержит текст или пустой", dealID)
				continue
			}

			parsed, err := strconv.ParseInt(cleaned, 10, 64)
			if err != nil {
				log.Printf("Ошибка парсинга UserID для сделки %s (значение: %q): %v", dealID, v, err)
				continue
			}
			userID = parsed
		case int64:
			userID = v
		case int:
			userID = int64(v)
		case float64:
			userID = int64(v)
		default:
			userIDStr := getStringValue(row[1])
			// Убираем неразрывные пробелы
			userIDStr = strings.ReplaceAll(userIDStr, "\u00a0", "")
			userIDStr = strings.ReplaceAll(userIDStr, " ", "")
			userIDStr = strings.TrimSpace(userIDStr)

			if userIDStr == "" || strings.HasPrefix(strings.ToLower(userIDStr), "без") {
				log.Printf("Пропуск сделки %s: UserID содержит текст или пустой", dealID)
				continue
			}

			parsed, err := strconv.ParseInt(userIDStr, 10, 64)
			if err != nil {
				log.Printf("Ошибка парсинга UserID для сделки %s (значение: %q): %v", dealID, userIDStr, err)
				continue
			}
			userID = parsed
		}

		// Profit находится в колонке D (индекс 3), но если колонка C отсутствует из-за IMPORTRANGE,
		// то Profit может быть в индексе 2. Проверяем оба варианта.
		var profit float64
		var profitIndex int
		if len(row) >= 4 {
			// Стандартный случай: A, B, C, D
			profitIndex = 3
			profit = getFloatValue(row[3])
			log.Printf("Сделка %s: Profit из колонки D (индекс %d), raw значение: %v, parsed: %.2f",
				dealID, profitIndex, row[3], profit)
		} else if len(row) >= 3 {
			// Если колонка C отсутствует: A, B, D
			profitIndex = 2
			profit = getFloatValue(row[2])
			log.Printf("Сделка %s: Profit из колонки D (индекс %d, колонка C отсутствует), raw значение: %v, parsed: %.2f",
				dealID, profitIndex, row[2], profit)
		} else {
			log.Printf("Пропуск сделки %s: недостаточно колонок для Profit (len=%d, row=%v)", dealID, len(row), row)
			continue
		}

		if profit <= 0 {
			log.Printf("Пропуск сделки %s: Profit <= 0 (значение: %f, raw: %v)", dealID, profit, row[profitIndex])
			continue
		}

		withdrawals = append(withdrawals, Withdrawal{
			DealID: dealID,
			UserID: userID,
			Profit: profit,
		})
	}

	return withdrawals, nil
}

// CreateReferral создает запись в листе Рефералы
func (sc *SheetsClient) CreateReferral(ref *Referral) error {
	// Находим первую пустую строку
	rowIndex, err := sc.findFirstEmptyRow("Рефералы")
	if err != nil {
		return fmt.Errorf("ошибка поиска пустой строки: %w", err)
	}

	values := [][]interface{}{
		{
			fmt.Sprintf("%d", ref.RefID), // Колонка A: ID реферала
			ref.RefCode,                  // Колонка B: Код пригласившего
			ref.Profit,                   // Колонка C: Чистая прибыль реферала
			ref.DealID,                   // Колонка D: ID сделки
			ref.Bonus,                    // Колонка E: Бонус рефоводу
			ref.Date,                     // Колонка F: Дата начисления
		},
	}

	log.Printf("📝 Запись в Рефералы (строка %d): RefID=%d, RefCode=%s, Profit=%.2f, DealID=%s, Bonus=%.2f, Date=%s",
		rowIndex, ref.RefID, ref.RefCode, ref.Profit, ref.DealID, ref.Bonus, ref.Date)

	valueRange := &sheets.ValueRange{
		Values: values,
	}

	// Используем Update с конкретной строкой вместо Append
	updateRange := fmt.Sprintf("Рефералы!A%d:F%d", rowIndex, rowIndex)
	updateResp, err := sc.service.Spreadsheets.Values.Update(
		sc.spreadsheetID,
		updateRange,
		valueRange,
	).ValueInputOption("USER_ENTERED").Do()

	if err != nil {
		log.Printf("❌ Ошибка записи в Рефералы: %v", err)
		return fmt.Errorf("ошибка добавления в Рефералы: %w", err)
	}

	log.Printf("✅ Добавлена запись в Рефералы: DealID=%s, RefID=%d, код=%s, бонус=%.2f (строка %d)",
		ref.DealID, ref.RefID, ref.RefCode, ref.Bonus, rowIndex)
	if updateResp.UpdatedCells > 0 {
		log.Printf("   Обновлено ячеек: %d, диапазон: %s", updateResp.UpdatedCells, updateResp.UpdatedRange)
	}

	// Обновляем кэш DealIDs
	sc.cacheMutex.Lock()
	sc.existingDealIDs[ref.DealID] = true
	sc.cacheMutex.Unlock()

	return nil
}

// UpdatePendingPayouts обновляет столбец "Ожидает выплаты" (F) для всех рефоводов
// Формула: Ожидает выплаты = текущее значение - Выплачено (где Выплачено - это функция СУММ)
// Выполняется каждый час для синхронизации с выплатами
func (sc *SheetsClient) UpdatePendingPayouts() error {
	log.Printf("Начало обновления столбца 'Ожидает выплаты'...")

	readRange := "Рефоводы!A2:G"
	// Используем UNFORMATTED_VALUE для получения вычисленных значений из функций (например, СУММ)
	resp, err := sc.service.Spreadsheets.Values.Get(sc.spreadsheetID, readRange).
		ValueRenderOption("UNFORMATTED_VALUE").Do()
	if err != nil {
		return fmt.Errorf("ошибка чтения листа Рефоводы: %w", err)
	}

	if len(resp.Values) == 0 {
		log.Printf("Нет данных для обновления")
		return nil
	}

	var updates []*sheets.ValueRange
	for i, row := range resp.Values {
		if len(row) < 1 {
			continue
		}

		// Пропускаем строки без ID
		idStr := getStringValue(row[0])
		if idStr == "" {
			continue
		}

		// Читаем текущее значение "Ожидает выплаты" (колонка F, индекс 5)
		var currentPending float64
		if len(row) > 5 {
			currentPending = getFloatValue(row[5])
		}

		// Читаем "Выплачено" (колонка G, индекс 6) - это вычисляемое значение из функции СУММ
		var paidOut float64
		if len(row) > 6 {
			paidOut = getFloatValue(row[6])
		}

		// Вычисляем новое значение: Ожидает выплаты - Выплачено
		newPending := currentPending - paidOut

		// Если значение изменилось, обновляем
		if newPending != currentPending {
			rowIndex := i + 2 // +2 потому что начинаем с строки 2 и индексация с 0
			updateRange := fmt.Sprintf("Рефоводы!F%d", rowIndex)

			updates = append(updates, &sheets.ValueRange{
				Range:  updateRange,
				Values: [][]interface{}{{newPending}},
			})

			log.Printf("Обновление строки %d (ID: %s): Ожидает выплаты %.2f -> %.2f (Выплачено: %.2f)",
				rowIndex, idStr, currentPending, newPending, paidOut)
		}
	}

	if len(updates) == 0 {
		log.Printf("Нет изменений для обновления")
		return nil
	}

	// Выполняем batch update
	body := &sheets.BatchUpdateValuesRequest{
		ValueInputOption: "USER_ENTERED",
		Data:             updates,
	}

	updateResp, err := sc.service.Spreadsheets.Values.BatchUpdate(sc.spreadsheetID, body).Do()
	if err != nil {
		return fmt.Errorf("ошибка обновления столбца 'Ожидает выплаты': %w", err)
	}

	log.Printf("Обновлено строк: %d", len(updates))
	if updateResp.TotalUpdatedCells > 0 {
		log.Printf("Обновлено ячеек: %d", updateResp.TotalUpdatedCells)
	}

	return nil
}

// Helper functions
func getStringValue(val interface{}) string {
	if val == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprintf("%v", val))
}

func getIntValue(val interface{}) int {
	if val == nil {
		return 0
	}

	// Пробуем разные типы
	switch v := val.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		if v == "" {
			return 0
		}
		result, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return 0
		}
		return result
	default:
		// Пробуем через строку
		str := getStringValue(val)
		if str == "" {
			return 0
		}
		result, err := strconv.Atoi(str)
		if err != nil {
			return 0
		}
		return result
	}
}

func getFloatValue(val interface{}) float64 {
	if val == nil {
		return 0.0
	}

	// Пробуем разные типы
	switch v := val.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		if v == "" {
			return 0.0
		}
		result, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0.0
		}
		return result
	default:
		// Пробуем через строку
		str := getStringValue(val)
		if str == "" {
			return 0.0
		}
		result, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return 0.0
		}
		return result
	}
}

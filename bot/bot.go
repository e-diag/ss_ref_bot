package bot

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"ss_ref_bot/config"
	"ss_ref_bot/sheets"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type Bot struct {
	api              *tgbotapi.BotAPI
	sheets           *sheets.SheetsClient
	waitingForWallet map[int64]bool
	mu               sync.RWMutex
}

var walletRegex = regexp.MustCompile(`^(UQ|EQ)[A-Za-z0-9_-]{46}$`)

func NewBot(token string, sheetsClient *sheets.SheetsClient) (*Bot, error) {
	api, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания бота: %w", err)
	}

	log.Printf("Авторизован как %s", api.Self.UserName)

	return &Bot{
		api:              api,
		sheets:           sheetsClient,
		waitingForWallet: make(map[int64]bool),
	}, nil
}

func (b *Bot) Start() error {
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := b.api.GetUpdatesChan(u)

	// Запускаем фоновую синхронизацию
	go b.startSyncWorker()

	// Запускаем фоновое обновление столбца "Ожидает выплаты" каждый час
	go b.startPayoutUpdateWorker()

	for update := range updates {
		go func(upd tgbotapi.Update) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Паника в обработке обновления: %v", r)
				}
			}()
			b.handleUpdate(upd)
		}(update)
	}

	return nil
}

func (b *Bot) handleUpdate(update tgbotapi.Update) {
	if update.Message == nil {
		return
	}

	msg := update.Message
	userID := msg.From.ID
	username := msg.From.UserName

	log.Printf("Сообщение от %d (@%s): %s", userID, username, msg.Text)

	// Обработка команды /start
	if msg.IsCommand() && msg.Command() == "start" {
		b.handleStart(msg, userID, username)
		return
	}

	// Обработка текстовых сообщений (для ввода кошелька)
	if msg.Text != "" {
		// Проверяем, ожидаем ли мы ввод кошелька
		b.mu.RLock()
		waiting := b.waitingForWallet[userID]
		b.mu.RUnlock()

		if waiting {
			b.handleWalletInput(msg, userID)
			return
		}

		// Если текст похож на адрес кошелька, но пользователь не нажимал кнопку,
		// проверяем формат и предлагаем сохранить
		if walletRegex.MatchString(strings.TrimSpace(msg.Text)) {
			// Проверяем, есть ли у пользователя рефовод
			ref, err := b.sheets.GetReferrerByID(userID)
			if err == nil && ref != nil && ref.Wallet == "" {
				b.sendMessage(msg.Chat.ID, "Обнаружен адрес кошелька. Используйте кнопку 'Подключить TON-кошелёк' для его сохранения.")
			}
		}
	}

	// Обработка кнопок
	if msg.Text == "Пригласить друзей" {
		b.handleInviteFriends(msg, userID, username)
		return
	}

	if msg.Text == "Мои рефералы" {
		b.handleMyReferrals(msg, userID)
		return
	}

	if msg.Text == "Подключить TON-кошелёк" || msg.Text == "Изменить кошелек" {
		b.handleConnectWallet(msg, userID)
		return
	}

	// Показываем меню для неизвестных команд
	b.showMenu(msg.Chat.ID, "Выберите действие из меню:")
}

func (b *Bot) handleStart(msg *tgbotapi.Message, userID int64, username string) {
	commandArgs := msg.CommandArguments()

	// Если есть аргумент (реферальный код)
	if commandArgs != "" {
		b.handleReferralLink(msg, userID, username, commandArgs)
		return
	}

	// Обычный /start
	ref, err := b.sheets.GetReferrerByID(userID)
	if err != nil {
		log.Printf("Ошибка получения рефовода: %v", err)
		b.sendMessage(msg.Chat.ID, "Произошла ошибка. Попробуйте позже.")
		return
	}

	// Если рефовод не существует, создаем его
	if ref == nil {
		// Проверяем наличие username
		if username == "" {
			b.sendMessage(msg.Chat.ID, "Для использования бота необходимо установить username в настройках Telegram.\n\nПосле установки username отправьте команду /start снова.")
			return
		}

		ref, err = b.sheets.CreateReferrer(userID, "@"+username)
		if err != nil {
			log.Printf("Ошибка создания рефовода: %v", err)
			b.sendMessage(msg.Chat.ID, "Произошла ошибка при регистрации. Попробуйте позже.")
			return
		}
	} else {
		// Проверяем и обновляем username, если он изменился
		b.updateUsernameIfChanged(ref, username)
	}

	// Отправляем приветственное сообщение
	negarantLink := "https://t.me/negarant_bot?startapp=ref_7968044364"
	welcomeMsg := fmt.Sprintf(`<b>Swap Stars | Обмен звёзд</b>

<b>⭐️Добро пожаловать в Swap Stars - сервис для обмена Telegram Stars на USDT!</b>
С помощью нашего сервиса вы можете продать свои звёзды и не ждать 21-дневный лок.
На данный момент звёзды продаются только за $USDT

<blockquote>Актуальный курс:

Сделки ДО 10000 звёзд⭐️

$1,14 - 100 звёзд

Сделки ОТ 10000 звёзд⭐️

$1,2 - 100 звёзд</blockquote>

😎В случае, если сделка должна проводиться через гаранта, то будет использоваться бот: <a href="%s">@negarant_bot</a>

<b>Через других гарантов сделки проводиться не будут!</b>

<b>✍️Для продажи звёзд обращайтесь к менеджеру: @SwapStars_Manager</b>`, negarantLink)

	b.sendHTMLMessage(msg.Chat.ID, welcomeMsg)
	b.showMenu(msg.Chat.ID, "")
}

func (b *Bot) handleReferralLink(msg *tgbotapi.Message, userID int64, username string, refCode string) {
	// Проверяем, не привязан ли уже пользователь
	invited, err := b.sheets.GetInvitedByUserID(userID)
	if err != nil {
		log.Printf("Ошибка проверки приглашенного: %v", err)
		b.sendMessage(msg.Chat.ID, "Произошла ошибка. Попробуйте позже.")
		return
	}

	if invited != nil {
		// Пользователь уже привязан
		b.sendMessage(msg.Chat.ID, "Вы уже привязаны к реферальной программе.")
		b.showMenu(msg.Chat.ID, "")
		return
	}

	// Проверяем существование рефовода с таким кодом
	ref, err := b.sheets.GetReferrerByCode(refCode)
	if err != nil {
		log.Printf("Ошибка получения рефовода по коду: %v", err)
		b.sendMessage(msg.Chat.ID, "Произошла ошибка. Попробуйте позже.")
		return
	}

	if ref == nil {
		b.sendMessage(msg.Chat.ID, "Неверный реферальный код.")
		return
	}

	// Проверяем, не пытается ли рефовод пригласить сам себя
	if ref.ID == userID {
		b.sendMessage(msg.Chat.ID, "Вы не можете использовать свою собственную реферальную ссылку.")
		b.showMenu(msg.Chat.ID, "")
		return
	}

	// Создаем запись в Приглашенные
	err = b.sheets.CreateInvited(userID, refCode)
	if err != nil {
		log.Printf("Ошибка создания записи в Приглашенные: %v", err)
		b.sendMessage(msg.Chat.ID, "Произошла ошибка. Попробуйте позже.")
		return
	}

	// Увеличиваем счетчик рефералов
	err = b.sheets.IncrementRefCount(refCode)
	if err != nil {
		log.Printf("Ошибка увеличения счетчика рефералов: %v", err)
		// Не критично, продолжаем
	}

	// Отправляем приветственное сообщение рефералу
	negarantLink := "https://t.me/negarant_bot?startapp=ref_7968044364"
	welcomeMsg := fmt.Sprintf(`<b>Swap Stars | Обмен звёзд</b>

<b>⭐️Добро пожаловать в Swap Stars - сервис для обмена Telegram Stars на USDT!</b>
С помощью нашего сервиса вы можете продать свои звёзды и не ждать 21-дневный лок.
На данный момент звёзды продаются только за $USDT

<blockquote>Актуальный курс:

Сделки ДО 10000 звёзд⭐️

$1,14 - 100 звёзд

Сделки ОТ 10000 звёзд⭐️

$1,2 - 100 звёзд</blockquote>

😎В случае, если сделка должна проводиться через гаранта, то будет использоваться бот: <a href="%s">@negarant_bot</a>

<b>Через других гарантов сделки проводиться не будут!</b>

<b>✍️Для продажи звёзд обращайтесь к менеджеру: @SwapStars_Manager</b>`, negarantLink)

	b.sendHTMLMessage(msg.Chat.ID, welcomeMsg)

	// Отправляем уведомление рефоводу о новом реферале
	referralUsername := username
	if referralUsername == "" {
		referralUsername = fmt.Sprintf("ID: %d", userID)
	} else {
		referralUsername = "@" + referralUsername
	}

	// Получаем обновленные данные рефовода (с новым счетчиком)
	updatedRef, err := b.sheets.GetReferrerByCode(refCode)
	if err != nil {
		log.Printf("Ошибка получения обновленных данных рефовода: %v", err)
		updatedRef = ref // Используем старые данные
	}

	notificationMsg := fmt.Sprintf(
		"*⭐️У вас новый реферал!*\n\n"+
			"%s\n\n"+
			"*Всего рефералов:* %d\n\n"+
			"*💸Приглашай друзей обменивать звезды и получай 10%% от прибыли с каждого друга!*\n\n"+
			"*Ваша реферальная ссылка:*\n\n"+
			"`%s`\n\n"+
			"/Мои рефералы",
		referralUsername,
		updatedRef.RefCount,
		fmt.Sprintf("https://t.me/%s?start=%s", b.api.Self.UserName, ref.Code),
	)

	b.sendFormattedMessage(ref.ID, notificationMsg)

	// Если пользователь еще не рефовод, создаем его
	existingRef, err := b.sheets.GetReferrerByID(userID)
	if err != nil {
		log.Printf("Ошибка проверки рефовода: %v", err)
	} else if existingRef == nil {
		// Создаем рефовода, если username есть
		if username != "" {
			_, err = b.sheets.CreateReferrer(userID, "@"+username)
			if err != nil {
				log.Printf("Ошибка создания рефовода: %v", err)
			}
		}
	} else {
		// Проверяем и обновляем username, если он изменился
		b.updateUsernameIfChanged(existingRef, username)
	}

	b.showMenu(msg.Chat.ID, "")
}

// updateUsernameIfChanged проверяет и обновляет username, если он изменился
func (b *Bot) updateUsernameIfChanged(ref *sheets.Referrer, currentUsername string) {
	if currentUsername == "" {
		return // Если username пустой, не обновляем
	}

	currentUsernameWithAt := "@" + currentUsername
	storedUsername := strings.TrimSpace(ref.Username)

	// Если username изменился, обновляем его в таблице
	if storedUsername != currentUsernameWithAt {
		log.Printf("Обновление username для ID %d: %s -> %s", ref.ID, storedUsername, currentUsernameWithAt)
		ref.Username = currentUsernameWithAt
		err := b.sheets.UpdateReferrer(ref)
		if err != nil {
			log.Printf("Ошибка обновления username: %v", err)
		} else {
			log.Printf("✅ Username успешно обновлен для ID %d", ref.ID)
		}
	}
}

func (b *Bot) handleInviteFriends(msg *tgbotapi.Message, userID int64, username string) {
	ref, err := b.sheets.GetReferrerByID(userID)
	if err != nil {
		log.Printf("Ошибка получения рефовода: %v", err)
		b.sendMessage(msg.Chat.ID, "Произошла ошибка. Попробуйте позже.")
		return
	}

	if ref == nil {
		// Создаем рефовода, если его нет
		if username == "" {
			b.sendMessage(msg.Chat.ID, "Для генерации реферальной ссылки необходимо установить username в настройках Telegram.")
			return
		}

		ref, err = b.sheets.CreateReferrer(userID, "@"+username)
		if err != nil {
			log.Printf("Ошибка создания рефовода: %v", err)
			b.sendMessage(msg.Chat.ID, "Произошла ошибка. Попробуйте позже.")
			return
		}
	} else {
		// Проверяем и обновляем username, если он изменился
		b.updateUsernameIfChanged(ref, username)
	}

	// Проверяем наличие username
	if ref.Username == "" || ref.Username == "@" {
		b.sendMessage(msg.Chat.ID, "Для генерации реферальной ссылки необходимо установить username в настройках Telegram.")
		return
	}

	botUsername := b.api.Self.UserName
	refLink := fmt.Sprintf("https://t.me/%s?start=%s", botUsername, ref.Code)

	message := fmt.Sprintf(
		"*💸Приглашай друзей обменивать звезды и получай 10%% от прибыли с каждого друга!*\n\n"+
			"*Ваша реферальная ссылка:*\n\n"+
			"`%s`",
		refLink,
	)

	b.sendFormattedMessage(msg.Chat.ID, message)
}

func (b *Bot) handleMyReferrals(msg *tgbotapi.Message, userID int64) {
	ref, err := b.sheets.GetReferrerByID(userID)
	if err != nil {
		log.Printf("Ошибка получения рефовода: %v", err)
		b.sendMessage(msg.Chat.ID, "Произошла ошибка. Попробуйте позже.")
		return
	}

	if ref == nil {
		b.sendMessage(msg.Chat.ID, "Вы еще не зарегистрированы как рефовод. Используйте команду /start.")
		return
	}

	// Проверяем и обновляем username, если он изменился
	username := msg.From.UserName
	if username != "" {
		b.updateUsernameIfChanged(ref, username)
		// Перечитываем данные после обновления
		ref, err = b.sheets.GetReferrerByID(userID)
		if err != nil {
			log.Printf("Ошибка перечитывания рефовода: %v", err)
		}
	}

	walletInfo := "не привязан"
	if ref.Wallet != "" {
		walletInfo = ref.Wallet
	}

	message := fmt.Sprintf(
		"<b>📊 Статистика рефералов</b>\n\n"+
			"<b>Количество рефералов:</b> %d\n"+
			"<b>Ожидает выплаты:</b> %.2f USDT\n"+
			"<b>Выплачено:</b> %.2f USDT\n"+
			"<b>Кошелёк:</b> %s",
		ref.RefCount,
		ref.PendingPayout,
		ref.PaidOut,
		walletInfo,
	)

	b.sendHTMLMessage(msg.Chat.ID, message)
}

func (b *Bot) handleConnectWallet(msg *tgbotapi.Message, userID int64) {
	ref, err := b.sheets.GetReferrerByID(userID)
	if err != nil {
		log.Printf("Ошибка получения рефовода: %v", err)
		b.sendMessage(msg.Chat.ID, "Произошла ошибка. Попробуйте позже.")
		return
	}

	if ref == nil {
		b.sendMessage(msg.Chat.ID, "Вы еще не зарегистрированы как рефовод. Используйте команду /start.")
		return
	}

	// Проверяем и обновляем username, если он изменился
	username := msg.From.UserName
	if username != "" {
		b.updateUsernameIfChanged(ref, username)
	}

	// Устанавливаем флаг ожидания ввода кошелька
	b.mu.Lock()
	b.waitingForWallet[userID] = true
	b.mu.Unlock()

	b.sendMessage(msg.Chat.ID, "Введите адрес вашего TON-кошелька (формат: UQ... или EQ...):")
}

func (b *Bot) handleWalletInput(msg *tgbotapi.Message, userID int64) {
	// Снимаем флаг ожидания ввода (в любом случае)
	defer func() {
		b.mu.Lock()
		delete(b.waitingForWallet, userID)
		b.mu.Unlock()
	}()

	wallet := strings.TrimSpace(msg.Text)

	// Если пользователь отправил команду или кнопку, отменяем ввод
	if msg.Text == "Пригласить друзей" || msg.Text == "Мои рефералы" || msg.Text == "Подключить TON-кошелёк" || msg.Text == "Изменить кошелек" || msg.IsCommand() {
		return
	}

	if !walletRegex.MatchString(wallet) {
		b.sendMessage(msg.Chat.ID, "Неверный формат адреса кошелька. Используйте формат: UQ... или EQ... (48 символов)\n\nПопробуйте еще раз или используйте кнопки меню.")
		// Устанавливаем флаг обратно для повторной попытки
		b.mu.Lock()
		b.waitingForWallet[userID] = true
		b.mu.Unlock()
		return
	}

	ref, err := b.sheets.GetReferrerByID(userID)
	if err != nil {
		log.Printf("Ошибка получения рефовода: %v", err)
		b.sendMessage(msg.Chat.ID, "Произошла ошибка. Попробуйте позже.")
		return
	}

	if ref == nil {
		b.sendMessage(msg.Chat.ID, "Вы еще не зарегистрированы как рефовод.")
		return
	}

	ref.Wallet = wallet
	err = b.sheets.UpdateReferrer(ref)
	if err != nil {
		log.Printf("Ошибка обновления кошелька: %v", err)
		b.sendMessage(msg.Chat.ID, "Произошла ошибка при сохранении кошелька. Попробуйте позже.")
		return
	}

	b.sendMessage(msg.Chat.ID, fmt.Sprintf("✅ TON-кошелёк успешно подключен:\n%s", wallet))
}

func (b *Bot) showMenu(chatID int64, text string) {
	// Получаем информацию о рефоводе для определения текста кнопки кошелька
	// В Telegram chatID == userID для личных чатов
	ref, err := b.sheets.GetReferrerByID(chatID)
	walletButtonText := "Подключить TON-кошелёк"
	if err == nil && ref != nil && ref.Wallet != "" {
		walletButtonText = "Изменить кошелек"
	}

	keyboard := tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("Пригласить друзей"),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("Мои рефералы"),
			tgbotapi.NewKeyboardButton(walletButtonText),
		),
	)

	keyboard.ResizeKeyboard = true

	// Если текст пустой, не отправляем сообщение, только обновляем клавиатуру
	if text == "" {
		// Отправляем пустое сообщение только для обновления клавиатуры
		msg := tgbotapi.NewMessage(chatID, "")
		msg.ReplyMarkup = keyboard
		_, err = b.api.Send(msg)
		if err != nil {
			log.Printf("Ошибка обновления клавиатуры: %v", err)
		}
	} else {
		msg := tgbotapi.NewMessage(chatID, text)
		msg.ReplyMarkup = keyboard
		_, err = b.api.Send(msg)
		if err != nil {
			log.Printf("Ошибка отправки меню: %v", err)
		}
	}
}

func (b *Bot) sendMessage(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	_, err := b.api.Send(msg)
	if err != nil {
		log.Printf("Ошибка отправки сообщения: %v", err)
	}
}

func (b *Bot) sendFormattedMessage(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = tgbotapi.ModeMarkdown
	msg.DisableWebPagePreview = true // Отключаем превью ссылок
	_, err := b.api.Send(msg)
	if err != nil {
		log.Printf("Ошибка отправки форматированного сообщения: %v", err)
		// Пробуем отправить без форматирования
		plainText := strings.ReplaceAll(text, "*", "")
		plainText = strings.ReplaceAll(plainText, "`", "")
		plainText = strings.ReplaceAll(plainText, "> ", "")
		plainText = strings.ReplaceAll(plainText, "[", "")
		plainText = strings.ReplaceAll(plainText, "](", "")
		plainText = strings.ReplaceAll(plainText, ")", "")
		b.sendMessage(chatID, plainText)
	}
}

func (b *Bot) sendHTMLMessage(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = tgbotapi.ModeHTML
	msg.DisableWebPagePreview = true // Отключаем превью ссылок
	_, err := b.api.Send(msg)
	if err != nil {
		log.Printf("Ошибка отправки HTML сообщения: %v", err)
		// Пробуем отправить без форматирования
		plainText := strings.ReplaceAll(text, "<b>", "")
		plainText = strings.ReplaceAll(plainText, "</b>", "")
		plainText = strings.ReplaceAll(plainText, "<i>", "")
		plainText = strings.ReplaceAll(plainText, "</i>", "")
		plainText = strings.ReplaceAll(plainText, "<a href=\"", "")
		plainText = strings.ReplaceAll(plainText, "\">", "")
		plainText = strings.ReplaceAll(plainText, "</a>", "")
		b.sendMessage(chatID, plainText)
	}
}

// startSyncWorker запускает фоновую синхронизацию
func (b *Bot) startSyncWorker() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Паника в синхронизации: %v", r)
			// Перезапускаем через некоторое время
			time.Sleep(5 * time.Minute)
			go b.startSyncWorker()
		}
	}()

	interval := time.Duration(config.AppConfig.SyncIntervalHours) * time.Hour
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Первый запуск через 1 минуту после старта
	time.Sleep(1 * time.Minute)

	// Обновляем кэш перед первой синхронизацией
	if err := b.sheets.LoadCache(); err != nil {
		log.Printf("Ошибка обновления кэша: %v", err)
	}

	b.syncWithdrawals()

	for range ticker.C {
		// Обновляем кэш каждые 2 часа вместе с синхронизацией
		if err := b.sheets.LoadCache(); err != nil {
			log.Printf("Ошибка обновления кэша: %v", err)
		}
		b.syncWithdrawals()
	}
}

// startPayoutUpdateWorker запускает фоновое обновление столбца "Ожидает выплаты" каждый час
func (b *Bot) startPayoutUpdateWorker() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Паника в обновлении выплат: %v", r)
			// Перезапускаем через некоторое время
			time.Sleep(5 * time.Minute)
			go b.startPayoutUpdateWorker()
		}
	}()

	// Обновляем каждый час
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	// Первый запуск через 5 минут после старта
	time.Sleep(5 * time.Minute)
	b.updatePendingPayouts()

	for range ticker.C {
		b.updatePendingPayouts()
	}
}

func (b *Bot) updatePendingPayouts() {
	log.Printf("Начало обновления столбца 'Ожидает выплаты'...")

	defer func() {
		if r := recover(); r != nil {
			log.Printf("Паника в обновлении выплат: %v", r)
		}
	}()

	err := b.sheets.UpdatePendingPayouts()
	if err != nil {
		log.Printf("Ошибка обновления столбца 'Ожидает выплаты': %v", err)
	} else {
		log.Printf("Обновление столбца 'Ожидает выплаты' завершено успешно")
	}
}

func (b *Bot) syncWithdrawals() {
	log.Printf("Начало синхронизации выводов...")

	defer func() {
		if r := recover(); r != nil {
			log.Printf("Паника в синхронизации выводов: %v", r)
		}
	}()

	// Получаем новые выводы
	withdrawals, err := b.sheets.GetNewWithdrawals()
	if err != nil {
		log.Printf("Ошибка получения новых выводов: %v", err)
		return
	}

	if len(withdrawals) == 0 {
		log.Printf("Новых выводов не найдено")
		return
	}

	log.Printf("Найдено новых выводов: %d", len(withdrawals))

	// Обрабатываем каждый вывод
	for _, withdrawal := range withdrawals {
		err := b.processWithdrawal(withdrawal)
		if err != nil {
			log.Printf("Ошибка обработки вывода %s: %v", withdrawal.DealID, err)
			continue
		}
	}

	log.Printf("Синхронизация завершена")
}

func (b *Bot) processWithdrawal(withdrawal sheets.Withdrawal) error {
	log.Printf("Обработка вывода: DealID=%s, UserID=%d (из колонки B листа Выводы), Profit=%.2f",
		withdrawal.DealID, withdrawal.UserID, withdrawal.Profit)

	// Шаг 1: Находим реферала по ID пользователя в Приглашенные
	// Сверяем ID пользователя из колонки B листа "Выводы" с колонкой A листа "Приглашенные"
	invited, err := b.sheets.GetInvitedByUserID(withdrawal.UserID)
	if err != nil {
		return fmt.Errorf("ошибка поиска приглашенного: %w", err)
	}

	if invited == nil {
		log.Printf("⚠️ Пользователь %d (из Выводы, колонка B) не найден в Приглашенные (колонка A), пропускаем сделку %s",
			withdrawal.UserID, withdrawal.DealID)
		return nil
	}

	log.Printf("✅ Найден в Приглашенные: UserID=%d, код пригласившего='%s'",
		invited.UserID, invited.RefCode)

	// Шаг 2: Получаем рефовода по коду пригласившего
	log.Printf("🔍 Поиск рефовода с кодом '%s' в таблице Рефоводы...", invited.RefCode)
	ref, err := b.sheets.GetReferrerByCode(invited.RefCode)
	if err != nil {
		log.Printf("❌ Ошибка получения рефовода с кодом '%s': %v", invited.RefCode, err)
		return fmt.Errorf("ошибка получения рефовода: %w", err)
	}

	if ref == nil {
		log.Printf("⚠️ Рефовод с кодом '%s' не найден в таблице Рефоводы, пропускаем сделку %s",
			invited.RefCode, withdrawal.DealID)
		return nil
	}

	log.Printf("✅ Рефовод найден: ID=%d, Code=%s, Username=%s", ref.ID, ref.Code, ref.Username)

	// Шаг 3: Считаем бонус (10% от прибыли)
	bonus := withdrawal.Profit * 0.1
	log.Printf("💰 Расчет бонуса: прибыль=%.2f, бонус (10%%)=%.2f USDT", withdrawal.Profit, bonus)

	// Шаг 4: Создаем запись в Рефералы
	referral := &sheets.Referral{
		RefID:   withdrawal.UserID, // ID реферала (из колонки B Выводы)
		RefCode: invited.RefCode,   // Код пригласившего (из колонки B Приглашенные)
		Profit:  withdrawal.Profit, // Прибыль (из колонки D Выводы)
		DealID:  withdrawal.DealID, // ID сделки (из колонки A Выводы)
		Bonus:   bonus,             // Бонус рефоводу (10% от прибыли)
		Date:    time.Now().Format("02.01.2006 15:04"),
	}

	err = b.sheets.CreateReferral(referral)
	if err != nil {
		return fmt.Errorf("ошибка создания записи в Рефералы: %w", err)
	}

	log.Printf("✅ Запись создана в Рефералы: RefID=%d, RefCode=%s, DealID=%s, Bonus=%.2f",
		referral.RefID, referral.RefCode, referral.DealID, referral.Bonus)

	// Шаг 5: Добавляем бонус к ожидающей выплате рефовода
	oldPayout := ref.PendingPayout
	ref.PendingPayout += bonus
	err = b.sheets.UpdateReferrer(ref)
	if err != nil {
		return fmt.Errorf("ошибка обновления рефовода: %w", err)
	}

	log.Printf("✅ Рефовод обновлен: ID=%d, код=%s, ожидает выплаты: %.2f → %.2f USDT",
		ref.ID, ref.Code, oldPayout, ref.PendingPayout)

	log.Printf("✅ Вывод полностью обработан: сделка %s, реферал %d, бонус %.2f USDT",
		withdrawal.DealID, withdrawal.UserID, bonus)

	return nil
}

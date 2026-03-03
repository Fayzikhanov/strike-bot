type TelegramThemeParams = {
  bg_color?: string
  text_color?: string
  hint_color?: string
  link_color?: string
  button_color?: string
  button_text_color?: string
  secondary_bg_color?: string
  header_bg_color?: string
}

type TelegramWebApp = {
  ready: () => void
  expand: () => void
  disableVerticalSwipes?: () => void
  setHeaderColor?: (color: string) => void
  setBackgroundColor?: (color: string) => void
  themeParams?: TelegramThemeParams
}

declare global {
  interface Window {
    Telegram?: {
      WebApp?: TelegramWebApp
    }
  }
}

export function initTelegramWebApp(): void {
  const webApp = window.Telegram?.WebApp
  if (!webApp) {
    return
  }

  webApp.ready()
  webApp.expand()
  webApp.disableVerticalSwipes?.()
  webApp.setHeaderColor?.('#121212')
  webApp.setBackgroundColor?.('#121212')
}

import { ChangeEvent, useCallback, useEffect, useMemo, useState } from "react";
import { Navigate } from "react-router-dom";
import {
  createAdminBroadcastCampaign,
  fetchAdminBroadcastCampaign,
  fetchAdminBroadcastCampaigns,
  previewAdminBroadcast,
  type AdminBroadcastCampaignItem,
  type AdminBroadcastCreatePayload,
  type AdminBroadcastFilters,
  type AdminBroadcastMode,
  type AdminBroadcastPreviewPayload,
} from "../api/strikeApi";
import { readSavedAdminKey } from "../lib/adminAuth";

function formatDateTime(timestamp: number): string {
  const safe = Number.isFinite(timestamp) ? Math.max(0, Math.floor(timestamp)) : 0;
  if (safe <= 0) {
    return "—";
  }
  const date = new Date(safe * 1000);
  return date.toLocaleString("ru-RU", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function parseUserIds(input: string): number[] {
  return Array.from(
    new Set(
      String(input || "")
        .split(/[\s,;\n]+/)
        .map((item) => Number.parseInt(item.trim(), 10))
        .filter((item) => Number.isFinite(item) && item > 0),
    ),
  );
}

function parseUsernames(input: string): string[] {
  return Array.from(
    new Set(
      String(input || "")
        .split(/[\s,;\n]+/)
        .map((item) => item.trim().replace(/^@+/, ""))
        .filter(Boolean),
    ),
  );
}

function fileToDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result || "");
      if (!result) {
        reject(new Error("Не удалось прочитать файл"));
        return;
      }
      resolve(result);
    };
    reader.onerror = () => reject(new Error("Не удалось прочитать файл"));
    reader.readAsDataURL(file);
  });
}

export function AdminBroadcasts() {
  const adminKey = readSavedAdminKey().trim();

  const [mode, setMode] = useState<AdminBroadcastMode>("mass");
  const [createdBy, setCreatedBy] = useState("dashboard-admin");

  const [welcomeBonus, setWelcomeBonus] = useState<"any" | "claimed" | "not_claimed">("any");
  const [balance, setBalance] = useState<"any" | "positive" | "zero">("any");
  const [activePrivileges, setActivePrivileges] = useState<"any" | "yes" | "no">("any");
  const [activityMode, setActivityMode] = useState<"any" | "active" | "inactive">("any");
  const [activityDays, setActivityDays] = useState("7");
  const [purchasePrivilege, setPurchasePrivilege] = useState("");
  const [purchaseServer, setPurchaseServer] = useState("");

  const [targetUserIdsInput, setTargetUserIdsInput] = useState("");
  const [targetUsernamesInput, setTargetUsernamesInput] = useState("");

  const [textRu, setTextRu] = useState("");
  const [textUz, setTextUz] = useState("");
  const [buttonUrl, setButtonUrl] = useState("");
  const [buttonTextRu, setButtonTextRu] = useState("");
  const [buttonTextUz, setButtonTextUz] = useState("");

  const [photoDataUrl, setPhotoDataUrl] = useState("");
  const [photoName, setPhotoName] = useState("");
  const [photoMimeType, setPhotoMimeType] = useState("");

  const [previewToken, setPreviewToken] = useState("");
  const [previewRecipientsCount, setPreviewRecipientsCount] = useState(0);
  const [previewLanguageStats, setPreviewLanguageStats] = useState({ ru: 0, uz: 0 });
  const [previewAudienceLabel, setPreviewAudienceLabel] = useState("");
  const [previewSample, setPreviewSample] = useState<Array<{ userId: number; username: string; displayName: string; language: "ru" | "uz" }>>([]);
  const [previewMissingIds, setPreviewMissingIds] = useState<number[]>([]);
  const [previewMissingUsernames, setPreviewMissingUsernames] = useState<string[]>([]);

  const [confirmSend, setConfirmSend] = useState(false);
  const [confirmPhrase, setConfirmPhrase] = useState("");

  const [isUploadingPhoto, setIsUploadingPhoto] = useState(false);
  const [isPreviewing, setIsPreviewing] = useState(false);
  const [isCreating, setIsCreating] = useState(false);
  const [isLoadingCampaigns, setIsLoadingCampaigns] = useState(false);

  const [campaigns, setCampaigns] = useState<AdminBroadcastCampaignItem[]>([]);
  const [selectedCampaign, setSelectedCampaign] = useState<AdminBroadcastCampaignItem | null>(null);

  const [errorMessage, setErrorMessage] = useState("");
  const [successMessage, setSuccessMessage] = useState("");

  const targetStats = useMemo(() => {
    const userIds = parseUserIds(targetUserIdsInput);
    const usernames = parseUsernames(targetUsernamesInput);
    return {
      userIds,
      usernames,
    };
  }, [targetUserIdsInput, targetUsernamesInput]);

  const resetPreview = () => {
    setPreviewToken("");
    setPreviewRecipientsCount(0);
    setPreviewLanguageStats({ ru: 0, uz: 0 });
    setPreviewAudienceLabel("");
    setPreviewSample([]);
    setPreviewMissingIds([]);
    setPreviewMissingUsernames([]);
    setConfirmSend(false);
    setConfirmPhrase("");
  };

  const buildPreviewPayload = useCallback((): AdminBroadcastPreviewPayload => {
    const filters: AdminBroadcastFilters = {
      welcomeBonus,
      balance,
      activePrivileges,
      activityMode,
      activityDays: Math.max(Number.parseInt(activityDays.trim() || "0", 10) || 0, 1),
      purchasePrivilege: purchasePrivilege.trim(),
      purchaseServer: purchaseServer.trim(),
    };

    return {
      mode,
      createdBy: createdBy.trim() || "dashboard-admin",
      filters,
      target: {
        userIds: targetStats.userIds,
        usernames: targetStats.usernames,
      },
      content: {
        textRu: textRu.trim(),
        textUz: textUz.trim(),
        photoDataUrl,
        photoName,
        photoMimeType,
        buttonUrl: buttonUrl.trim(),
        buttonTextRu: buttonTextRu.trim(),
        buttonTextUz: buttonTextUz.trim(),
      },
    };
  }, [
    activityDays,
    activityMode,
    activePrivileges,
    balance,
    buttonTextRu,
    buttonTextUz,
    buttonUrl,
    createdBy,
    mode,
    photoDataUrl,
    photoMimeType,
    photoName,
    purchasePrivilege,
    purchaseServer,
    targetStats.userIds,
    targetStats.usernames,
    textRu,
    textUz,
    welcomeBonus,
  ]);

  const loadCampaigns = useCallback(async () => {
    if (!adminKey) {
      return;
    }
    setIsLoadingCampaigns(true);
    try {
      const response = await fetchAdminBroadcastCampaigns(adminKey, 40);
      setCampaigns(response.items);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Не удалось загрузить кампании";
      setErrorMessage(message || "Не удалось загрузить кампании");
    } finally {
      setIsLoadingCampaigns(false);
    }
  }, [adminKey]);

  useEffect(() => {
    void loadCampaigns();
    const interval = window.setInterval(() => {
      void loadCampaigns();
    }, 15000);
    return () => {
      window.clearInterval(interval);
    };
  }, [loadCampaigns]);

  const handlePhotoUpload = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }
    setIsUploadingPhoto(true);
    setErrorMessage("");
    try {
      const dataUrl = await fileToDataUrl(file);
      setPhotoDataUrl(dataUrl);
      setPhotoName(file.name || "campaign-image");
      setPhotoMimeType(file.type || "image/jpeg");
      resetPreview();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Не удалось загрузить фото";
      setErrorMessage(message || "Не удалось загрузить фото");
    } finally {
      setIsUploadingPhoto(false);
      event.target.value = "";
    }
  };

  const handlePreview = async () => {
    if (!adminKey) {
      return;
    }
    setIsPreviewing(true);
    setErrorMessage("");
    setSuccessMessage("");
    try {
      const payload = buildPreviewPayload();
      const response = await previewAdminBroadcast(adminKey, payload);
      const preview = response.preview;
      setPreviewToken(preview.previewToken);
      setPreviewRecipientsCount(preview.audience.totalRecipients);
      setPreviewLanguageStats({
        ru: Number(preview.audience.languageStats?.ru || 0),
        uz: Number(preview.audience.languageStats?.uz || 0),
      });
      setPreviewAudienceLabel(preview.campaign.audienceLabel);
      setPreviewSample(preview.audience.sampleRecipients);
      setPreviewMissingIds(preview.audience.missingUserIds);
      setPreviewMissingUsernames(preview.audience.missingUsernames);
      setConfirmSend(false);
      setConfirmPhrase("");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Не удалось выполнить preview";
      setErrorMessage(message || "Не удалось выполнить preview");
    } finally {
      setIsPreviewing(false);
    }
  };

  const handleCreateCampaign = async () => {
    if (!adminKey || !previewToken) {
      return;
    }
    setIsCreating(true);
    setErrorMessage("");
    setSuccessMessage("");
    try {
      const payload: AdminBroadcastCreatePayload = {
        previewToken,
        confirmSend,
        confirmPhrase: confirmPhrase.trim(),
      };
      const response = await createAdminBroadcastCampaign(adminKey, payload);
      setSuccessMessage(`Кампания ${response.campaign.id} поставлена в очередь`);
      setPreviewToken("");
      setConfirmSend(false);
      setConfirmPhrase("");
      await loadCampaigns();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Не удалось создать кампанию";
      setErrorMessage(message || "Не удалось создать кампанию");
    } finally {
      setIsCreating(false);
    }
  };

  const handleOpenCampaign = async (campaignId: string) => {
    if (!adminKey) {
      return;
    }
    setErrorMessage("");
    try {
      const response = await fetchAdminBroadcastCampaign(adminKey, campaignId, 800);
      setSelectedCampaign(response.campaign);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Не удалось загрузить детали кампании";
      setErrorMessage(message || "Не удалось загрузить детали кампании");
    }
  };

  if (!adminKey) {
    return <Navigate to="/admin/login" replace />;
  }

  return (
    <div className="mx-auto max-w-[1400px] px-4 py-6 sm:px-6 lg:px-8">
      <div className="space-y-6">
        <section className="rounded-2xl border border-[#26262c] bg-[#151518] p-5">
          <p className="text-xs uppercase tracking-[0.16em] text-[#8a8a90]">Рассылки</p>
          <h1 className="mt-2 text-3xl font-black uppercase">Telegram-кампании</h1>
          <p className="mt-2 text-sm text-[#a3a3ab]">
            Массовая, сегментная или точечная отправка с preview аудитории, подтверждением и логами.
          </p>
        </section>

        {errorMessage && (
          <div className="rounded-xl border border-[#7f1d1d] bg-[#3b1212] px-4 py-3 text-sm text-[#fca5a5]">
            {errorMessage}
          </div>
        )}

        {successMessage && (
          <div className="rounded-xl border border-[#1d7f4b] bg-[#113322] px-4 py-3 text-sm text-[#86efac]">
            {successMessage}
          </div>
        )}

        <section className="rounded-2xl border border-[#26262c] bg-[#151518] p-5">
          <h2 className="text-xl font-black uppercase">Новая кампания</h2>

          <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-3">
            <label className="space-y-1">
              <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Тип рассылки</span>
              <select
                value={mode}
                onChange={(event) => {
                  setMode(event.target.value as AdminBroadcastMode);
                  resetPreview();
                }}
                className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
              >
                <option value="mass">Массовая (всем)</option>
                <option value="segment">Сегментная (по фильтрам)</option>
                <option value="targeted">Точечная (user_id / username)</option>
              </select>
            </label>

            <label className="space-y-1 md:col-span-2">
              <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Кто запускает</span>
              <input
                type="text"
                value={createdBy}
                onChange={(event) => {
                  setCreatedBy(event.target.value);
                  resetPreview();
                }}
                className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
                placeholder="admin-dashboard"
              />
            </label>
          </div>

          {mode === "segment" && (
            <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
              <label className="space-y-1">
                <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Welcome-бонус</span>
                <select
                  value={welcomeBonus}
                  onChange={(event) => {
                    setWelcomeBonus(event.target.value as "any" | "claimed" | "not_claimed");
                    resetPreview();
                  }}
                  className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
                >
                  <option value="any">Любой</option>
                  <option value="claimed">Получал</option>
                  <option value="not_claimed">Не получал</option>
                </select>
              </label>

              <label className="space-y-1">
                <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Баланс</span>
                <select
                  value={balance}
                  onChange={(event) => {
                    setBalance(event.target.value as "any" | "positive" | "zero");
                    resetPreview();
                  }}
                  className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
                >
                  <option value="any">Любой</option>
                  <option value="positive">Больше 0</option>
                  <option value="zero">Равно 0</option>
                </select>
              </label>

              <label className="space-y-1">
                <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Активные привилегии</span>
                <select
                  value={activePrivileges}
                  onChange={(event) => {
                    setActivePrivileges(event.target.value as "any" | "yes" | "no");
                    resetPreview();
                  }}
                  className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
                >
                  <option value="any">Любой</option>
                  <option value="yes">Есть</option>
                  <option value="no">Нет</option>
                </select>
              </label>

              <label className="space-y-1">
                <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Активность</span>
                <select
                  value={activityMode}
                  onChange={(event) => {
                    setActivityMode(event.target.value as "any" | "active" | "inactive");
                    resetPreview();
                  }}
                  className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
                >
                  <option value="any">Любая</option>
                  <option value="active">Активен за N дней</option>
                  <option value="inactive">Неактивен за N дней</option>
                </select>
              </label>

              <label className="space-y-1">
                <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">N дней</span>
                <input
                  type="number"
                  min={1}
                  value={activityDays}
                  onChange={(event) => {
                    setActivityDays(event.target.value);
                    resetPreview();
                  }}
                  className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
                />
              </label>

              <label className="space-y-1">
                <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Покупал привилегию</span>
                <input
                  type="text"
                  value={purchasePrivilege}
                  onChange={(event) => {
                    setPurchasePrivilege(event.target.value);
                    resetPreview();
                  }}
                  placeholder="например: prime"
                  className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
                />
              </label>

              <label className="space-y-1 md:col-span-2 xl:col-span-3">
                <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Покупал сервер</span>
                <input
                  type="text"
                  value={purchaseServer}
                  onChange={(event) => {
                    setPurchaseServer(event.target.value);
                    resetPreview();
                  }}
                  placeholder="например: public style #1 или 27015"
                  className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
                />
              </label>
            </div>
          )}

          {mode === "targeted" && (
            <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
              <label className="space-y-1">
                <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">user_id (через запятую/пробел/новую строку)</span>
                <textarea
                  rows={4}
                  value={targetUserIdsInput}
                  onChange={(event) => {
                    setTargetUserIdsInput(event.target.value);
                    resetPreview();
                  }}
                  className="w-full resize-none rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
                />
              </label>
              <label className="space-y-1">
                <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">username (через запятую/пробел/новую строку)</span>
                <textarea
                  rows={4}
                  value={targetUsernamesInput}
                  onChange={(event) => {
                    setTargetUsernamesInput(event.target.value);
                    resetPreview();
                  }}
                  className="w-full resize-none rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
                />
              </label>
            </div>
          )}

          <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
            <label className="space-y-1">
              <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Текст RU</span>
              <textarea
                rows={5}
                value={textRu}
                onChange={(event) => {
                  setTextRu(event.target.value);
                  resetPreview();
                }}
                className="w-full resize-none rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
              />
            </label>
            <label className="space-y-1">
              <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Текст UZ</span>
              <textarea
                rows={5}
                value={textUz}
                onChange={(event) => {
                  setTextUz(event.target.value);
                  resetPreview();
                }}
                className="w-full resize-none rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
              />
            </label>
          </div>

          <div className="mt-3 rounded-xl border border-[#2c2c33] bg-[#101013] px-3 py-2 text-xs text-[#9ea2ad]">
            Форматирование текста: `**жирный**`, `__курсив__`, `~~зачеркнутый~~`, `` `код` ``, ссылка `[текст](https://...)`.
            Пустые строки сохраняются. Если текст длинный и есть фото, текст отправится отдельным сообщением для аккуратных отступов.
          </div>

          <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-3">
            <label className="space-y-1 md:col-span-1">
              <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Кнопка-ссылка (опц.)</span>
              <input
                type="url"
                value={buttonUrl}
                onChange={(event) => {
                  setButtonUrl(event.target.value);
                  resetPreview();
                }}
                placeholder="https://..."
                className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
              />
            </label>
            <label className="space-y-1">
              <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Текст кнопки RU</span>
              <input
                type="text"
                value={buttonTextRu}
                onChange={(event) => {
                  setButtonTextRu(event.target.value);
                  resetPreview();
                }}
                className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
              />
            </label>
            <label className="space-y-1">
              <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Текст кнопки UZ</span>
              <input
                type="text"
                value={buttonTextUz}
                onChange={(event) => {
                  setButtonTextUz(event.target.value);
                  resetPreview();
                }}
                className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2.5 text-sm text-white"
              />
            </label>
          </div>

          <div className="mt-4 flex flex-wrap items-center gap-3">
            <label className="inline-flex cursor-pointer items-center gap-2 rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2 text-sm text-[#d6d6dd]">
              <input
                type="file"
                accept="image/png,image/jpeg,image/webp"
                onChange={handlePhotoUpload}
                className="hidden"
              />
              {isUploadingPhoto ? "Загрузка фото..." : photoDataUrl ? `Фото: ${photoName}` : "Добавить фото"}
            </label>
            {photoDataUrl && (
              <button
                type="button"
                onClick={() => {
                  setPhotoDataUrl("");
                  setPhotoName("");
                  setPhotoMimeType("");
                  resetPreview();
                }}
                className="rounded-xl border border-[#303038] bg-[#1b1b20] px-3 py-2 text-sm text-[#d0d0d6]"
              >
                Удалить фото
              </button>
            )}
          </div>

          <div className="mt-5 flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={() => {
                void handlePreview();
              }}
              disabled={isPreviewing || isCreating}
              className="rounded-xl bg-[#f08800] px-4 py-2.5 text-sm font-black uppercase tracking-[0.05em] text-[#111111] disabled:opacity-60"
            >
              {isPreviewing ? "Считаем аудиторию..." : "Preview получателей"}
            </button>

            <button
              type="button"
              onClick={() => {
                void handleCreateCampaign();
              }}
              disabled={!previewToken || isCreating || isPreviewing}
              className="rounded-xl border border-[#f08800]/35 bg-[#f08800]/12 px-4 py-2.5 text-sm font-semibold text-[#ffb861] disabled:opacity-50"
            >
              {isCreating ? "Создание..." : "Запустить рассылку"}
            </button>
          </div>

          {previewToken && (
            <div className="mt-4 rounded-xl border border-[#2d2d33] bg-[#111114] p-4">
              <p className="text-sm font-semibold text-[#f1f1f3]">Preview: {previewRecipientsCount} получателей</p>
              <p className="mt-1 text-xs text-[#9ea2ad]">
                Языки аудитории: RU {previewLanguageStats.ru} · UZ {previewLanguageStats.uz}
              </p>
              {previewAudienceLabel && <p className="mt-1 text-sm text-[#a2a2ab]">{previewAudienceLabel}</p>}

              {(previewMissingIds.length > 0 || previewMissingUsernames.length > 0) && (
                <div className="mt-2 text-xs text-[#fca5a5]">
                  {previewMissingIds.length > 0 && <div>Не найдены user_id: {previewMissingIds.join(", ")}</div>}
                  {previewMissingUsernames.length > 0 && (
                    <div>Не найдены username: {previewMissingUsernames.join(", ")}</div>
                  )}
                </div>
              )}

              {previewSample.length > 0 && (
                <div className="mt-3 max-h-48 space-y-1 overflow-y-auto rounded-lg border border-[#2a2a31] bg-[#0d0d10] p-2 text-xs text-[#c7c7ce]">
                  {previewSample.map((item) => (
                    <div key={`${item.userId}-${item.username}`} className="flex items-center justify-between gap-2">
                      <span>
                        {item.displayName} ({item.userId})
                        {item.username ? ` @${item.username}` : ""}
                      </span>
                      <span className="rounded-full border border-[#2f2f36] px-2 py-0.5 uppercase text-[#9ea0aa]">
                        {item.language}
                      </span>
                    </div>
                  ))}
                </div>
              )}

              <div className="mt-3 space-y-2">
                <label className="inline-flex items-center gap-2 text-sm text-[#d0d0d6]">
                  <input
                    type="checkbox"
                    checked={confirmSend}
                    onChange={(event) => setConfirmSend(event.target.checked)}
                    className="h-4 w-4"
                  />
                  Подтверждаю запуск рассылки
                </label>

                <div className="grid grid-cols-1 gap-2 md:grid-cols-[220px_1fr] md:items-center">
                  <span className="text-xs uppercase tracking-[0.12em] text-[#8a8a90]">Фраза подтверждения</span>
                  <input
                    type="text"
                    value={confirmPhrase}
                    onChange={(event) => setConfirmPhrase(event.target.value)}
                    placeholder="Введите SEND"
                    className="w-full rounded-xl border border-[#2f2f35] bg-[#0f0f10] px-3 py-2 text-sm text-white"
                  />
                </div>
              </div>
            </div>
          )}
        </section>

        <section className="rounded-2xl border border-[#26262c] bg-[#151518] p-5">
          <div className="flex items-center justify-between gap-2">
            <h2 className="text-xl font-black uppercase">Логи кампаний</h2>
            <button
              type="button"
              onClick={() => {
                void loadCampaigns();
              }}
              className="rounded-xl border border-[#303038] bg-[#1b1b20] px-3 py-2 text-xs font-semibold uppercase tracking-[0.08em] text-[#d0d0d6]"
            >
              {isLoadingCampaigns ? "Обновляем..." : "Обновить"}
            </button>
          </div>

          <div className="mt-4 space-y-2">
            {campaigns.map((campaign) => (
              <button
                key={campaign.id}
                type="button"
                onClick={() => {
                  void handleOpenCampaign(campaign.id);
                }}
                className="w-full rounded-xl border border-[#2c2c33] bg-[#101013] p-3 text-left hover:border-[#3a3a45]"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div>
                    <p className="text-sm font-semibold text-white">#{campaign.id}</p>
                    <p className="text-xs text-[#9ca0aa]">{campaign.audienceLabel || campaign.mode}</p>
                  </div>
                  <div className="text-right text-xs text-[#9ca0aa]">
                    <div>Статус: {campaign.status}</div>
                    <div>Создано: {formatDateTime(campaign.createdAt)}</div>
                  </div>
                </div>
                <div className="mt-2 grid grid-cols-2 gap-2 text-xs sm:grid-cols-5">
                  <div className="rounded-lg border border-[#2e2e35] bg-[#17171c] px-2 py-1">Всего: {campaign.stats.created}</div>
                  <div className="rounded-lg border border-[#1f5c3f] bg-[#0f2219] px-2 py-1 text-[#86efac]">Sent: {campaign.stats.sent}</div>
                  <div className="rounded-lg border border-[#7f1d1d] bg-[#301315] px-2 py-1 text-[#fca5a5]">Failed: {campaign.stats.failed}</div>
                  <div className="rounded-lg border border-[#4a4a52] bg-[#1c1c22] px-2 py-1 text-[#d1d5db]">Skipped: {campaign.stats.skipped}</div>
                  <div className="rounded-lg border border-[#2e2e35] bg-[#17171c] px-2 py-1">Processed: {campaign.stats.processed}</div>
                </div>
              </button>
            ))}

            {campaigns.length === 0 && (
              <div className="rounded-xl border border-[#29292e] bg-[#101013] p-6 text-sm text-[#b8b8c0]">
                Кампаний пока нет.
              </div>
            )}
          </div>
        </section>

        {selectedCampaign && (
          <section className="rounded-2xl border border-[#26262c] bg-[#151518] p-5">
            <div className="flex items-start justify-between gap-3">
              <div>
                <h3 className="text-lg font-black uppercase">Кампания #{selectedCampaign.id}</h3>
                <p className="text-sm text-[#a2a2ab]">{selectedCampaign.audienceLabel}</p>
              </div>
              <button
                type="button"
                onClick={() => setSelectedCampaign(null)}
                className="rounded-lg border border-[#303038] bg-[#1b1b20] px-3 py-1.5 text-xs font-semibold text-[#d0d0d6]"
              >
                Закрыть
              </button>
            </div>

            <div className="mt-3 grid grid-cols-2 gap-2 text-xs md:grid-cols-6">
              <div className="rounded-lg border border-[#2e2e35] bg-[#17171c] px-2 py-1">Status: {selectedCampaign.status}</div>
              <div className="rounded-lg border border-[#2e2e35] bg-[#17171c] px-2 py-1">Created: {selectedCampaign.stats.created}</div>
              <div className="rounded-lg border border-[#1f5c3f] bg-[#0f2219] px-2 py-1 text-[#86efac]">Sent: {selectedCampaign.stats.sent}</div>
              <div className="rounded-lg border border-[#7f1d1d] bg-[#301315] px-2 py-1 text-[#fca5a5]">Failed: {selectedCampaign.stats.failed}</div>
              <div className="rounded-lg border border-[#4a4a52] bg-[#1c1c22] px-2 py-1">Skipped: {selectedCampaign.stats.skipped}</div>
              <div className="rounded-lg border border-[#2e2e35] bg-[#17171c] px-2 py-1">Processed: {selectedCampaign.stats.processed}</div>
            </div>

            <div className="mt-3 max-h-80 space-y-1 overflow-y-auto rounded-xl border border-[#2b2b32] bg-[#0f0f12] p-2 text-xs">
              {(selectedCampaign.logs || []).map((log, index) => (
                <div key={`${log.timestamp}-${index}`} className="rounded-lg border border-[#25252c] bg-[#16161b] px-2 py-1.5 text-[#d4d4db]">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span className="font-semibold uppercase tracking-[0.08em] text-[#9ea3ad]">{log.status}</span>
                    <span className="text-[#8b8f99]">{formatDateTime(log.timestamp)}</span>
                  </div>
                  {log.userId && (
                    <div className="mt-1 text-[#c7c7ce]">
                      user_id: {log.userId}
                      {log.username ? ` (@${log.username})` : ""}
                    </div>
                  )}
                  {log.message && <div className="mt-1">{log.message}</div>}
                  {log.error && <div className="mt-1 text-[#fca5a5]">{log.error}</div>}
                </div>
              ))}

              {(selectedCampaign.logs || []).length === 0 && (
                <div className="rounded-lg border border-[#25252c] bg-[#16161b] px-2 py-2 text-[#a8a8b0]">
                  Логи пока отсутствуют.
                </div>
              )}
            </div>
          </section>
        )}
      </div>
    </div>
  );
}

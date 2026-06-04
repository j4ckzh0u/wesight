import fs from 'fs';
import os from 'os';
import path from 'path';
import { pathToFileURL } from 'url';

import type { IMMessage, WeixinGatewayStatus, WeixinOpenClawConfig } from './types';

type NativeWeixinReply = (conversationId: string, text: string) => Promise<boolean>;
type NativeWeixinMessageCallback = (message: IMMessage, replyFn: (text: string) => Promise<void>) => Promise<void>;

interface WeixinQrStartResult {
  qrDataUrl?: string;
  message: string;
  sessionKey?: string;
}

interface WeixinQrWaitResult {
  connected: boolean;
  message: string;
  accountId?: string;
}

interface WeixinPluginModules {
  login: {
    startWeixinLoginWithQr: (opts: {
      accountId?: string;
      apiBaseUrl: string;
      botType?: string;
      force?: boolean;
      verbose?: boolean;
    }) => Promise<{ qrcodeUrl?: string; message: string; sessionKey?: string }>;
    waitForWeixinLogin: (opts: {
      sessionKey: string;
      apiBaseUrl: string;
      timeoutMs?: number;
      verbose?: boolean;
    }) => Promise<{
      connected?: boolean;
      alreadyConnected?: boolean;
      botToken?: string;
      accountId?: string;
      baseUrl?: string;
      userId?: string;
      message: string;
    }>;
  };
  accounts: {
    DEFAULT_BASE_URL: string;
    listWeixinAccountIds: (cfg: Record<string, unknown>) => string[];
    loadWeixinAccount: (accountId: string) => { token?: string; baseUrl?: string; userId?: string } | null;
    saveWeixinAccount: (accountId: string, update: { token?: string; baseUrl?: string; userId?: string }) => void;
    registerWeixinAccountId: (accountId: string) => void;
    resolveWeixinAccount: (cfg: Record<string, unknown>, accountId?: string | null) => {
      accountId: string;
      baseUrl: string;
      cdnBaseUrl: string;
      token?: string;
      enabled: boolean;
      configured: boolean;
    };
  };
  api: {
    getUpdates: (opts: {
      baseUrl: string;
      token?: string;
      timeoutMs?: number;
      get_updates_buf?: string;
    }) => Promise<{
      ret?: number;
      errcode?: number;
      errmsg?: string;
      msgs?: unknown[];
      get_updates_buf?: string;
      longpolling_timeout_ms?: number;
    }>;
    notifyStart: (opts: { baseUrl: string; token?: string; timeoutMs?: number }) => Promise<unknown>;
    notifyStop: (opts: { baseUrl: string; token?: string; timeoutMs?: number }) => Promise<unknown>;
  };
  inbound: {
    weixinMessageToMsgContext: (msg: unknown, accountId: string) => {
      Body: string;
      From: string;
      To: string;
      AccountId: string;
      MessageSid: string;
      Timestamp?: number;
      context_token?: string;
    };
    setContextToken: (accountId: string, userId: string, token: string) => void;
    getContextToken: (accountId: string, userId: string) => string | undefined;
    restoreContextTokens: (accountId: string) => void;
  };
  send: {
    sendMessageWeixin: (params: {
      to: string;
      text: string;
      opts: {
        baseUrl: string;
        token?: string;
        timeoutMs?: number;
        contextToken?: string;
      };
    }) => Promise<{ messageId: string }>;
  };
  syncBuf: {
    getSyncBufFilePath: (accountId: string) => string;
    loadGetUpdatesBuf: (filePath: string) => string | undefined;
    saveGetUpdatesBuf: (filePath: string, getUpdatesBuf: string) => void;
  };
}

interface WeixinRuntimeAccount {
  accountId: string;
  baseUrl: string;
  token?: string;
  getUpdatesBuf: string;
  syncBufPath: string;
}

const DEFAULT_LONG_POLL_MS = 35_000;
const WEIXIN_CONVERSATION_PREFIX = 'weixin-native';

const normalizeAccountId = (value: string): string => (
  value.trim().replace(/@/g, '-').replace(/\./g, '-')
);

const getMessageStableId = (msg: Record<string, unknown>, accountId: string): string => {
  const raw = msg.message_id ?? msg.client_id ?? msg.seq ?? `${msg.from_user_id ?? ''}:${msg.create_time_ms ?? ''}`;
  return `${accountId}:${String(raw)}`;
};

const asRecord = (value: unknown): Record<string, unknown> => (
  value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}
);

export class NativeWeixinGateway {
  private messageCallback: NativeWeixinMessageCallback | null = null;
  private pluginModules: WeixinPluginModules | null = null;
  private pollingAbortController: AbortController | null = null;
  private pollingPromise: Promise<void> | null = null;
  private account: WeixinRuntimeAccount | null = null;
  private startedAt: number | null = null;
  private lastError: string | null = null;
  private lastInboundAt: number | null = null;
  private lastOutboundAt: number | null = null;
  private seenMessageIds = new Set<string>();

  setMessageCallback(callback: NativeWeixinMessageCallback): void {
    this.messageCallback = callback;
  }

  async start(config: WeixinOpenClawConfig): Promise<void> {
    await this.stop();
    if (!config.enabled) return;

    const modules = await this.loadPluginModules();
    const accountId = this.resolveAccountId(config, modules);
    if (!accountId) {
      throw new Error('Weixin account is not connected. Please scan the QR code first.');
    }

    const account = modules.accounts.resolveWeixinAccount(this.buildOpenClawConfig(config, accountId), accountId);
    if (!account.configured || !account.token) {
      throw new Error('Weixin credentials are not available. Please scan the QR code again.');
    }

    modules.inbound.restoreContextTokens(account.accountId);
    const syncBufPath = modules.syncBuf.getSyncBufFilePath(account.accountId);
    this.account = {
      accountId: account.accountId,
      baseUrl: account.baseUrl,
      token: account.token,
      syncBufPath,
      getUpdatesBuf: modules.syncBuf.loadGetUpdatesBuf(syncBufPath) ?? '',
    };
    this.startedAt = Date.now();
    this.lastError = null;
    this.pollingAbortController = new AbortController();

    void modules.api.notifyStart({ baseUrl: this.account.baseUrl, token: this.account.token })
      .catch(error => console.debug('[NativeWeixinGateway] notify start failed:', error));

    this.pollingPromise = this.pollLoop(this.pollingAbortController.signal)
      .catch(error => {
        if (!this.pollingAbortController?.signal.aborted) {
          this.lastError = error instanceof Error ? error.message : String(error);
          console.error('[NativeWeixinGateway] polling failed:', error);
        }
      });
  }

  async stop(): Promise<void> {
    const previousAccount = this.account;
    const modules = this.pluginModules;
    if (this.pollingAbortController) {
      this.pollingAbortController.abort();
    }
    if (this.pollingPromise) {
      await this.pollingPromise.catch((): void => undefined);
    }
    this.pollingAbortController = null;
    this.pollingPromise = null;
    this.startedAt = null;
    this.account = null;
    if (previousAccount && modules) {
      await modules.api.notifyStop({ baseUrl: previousAccount.baseUrl, token: previousAccount.token })
        .catch(error => console.debug('[NativeWeixinGateway] notify stop failed:', error));
    }
  }

  isConnected(): boolean {
    return Boolean(this.account && this.startedAt && !this.lastError);
  }

  getStatus(config?: WeixinOpenClawConfig): WeixinGatewayStatus {
    return {
      connected: this.isConnected(),
      startedAt: this.startedAt,
      lastError: this.lastError,
      lastInboundAt: this.lastInboundAt,
      lastOutboundAt: this.lastOutboundAt,
      accountId: this.account?.accountId ?? config?.accountId ?? null,
    };
  }

  async qrLoginStart(accountId?: string): Promise<WeixinQrStartResult> {
    try {
      const modules = await this.loadPluginModules();
      const savedBaseUrl = accountId ? modules.accounts.loadWeixinAccount(accountId)?.baseUrl?.trim() : '';
      const result = await modules.login.startWeixinLoginWithQr({
        accountId: accountId || undefined,
        apiBaseUrl: savedBaseUrl || modules.accounts.DEFAULT_BASE_URL,
        force: true,
        verbose: true,
      });
      return {
        qrDataUrl: result.qrcodeUrl,
        message: result.message,
        sessionKey: result.sessionKey,
      };
    } catch (error) {
      this.lastError = error instanceof Error ? error.message : String(error);
      return { message: `Failed to start Weixin login: ${this.lastError}` };
    }
  }

  async qrLoginWait(sessionKey: string, config?: WeixinOpenClawConfig): Promise<WeixinQrWaitResult> {
    try {
      const modules = await this.loadPluginModules();
      const result = await modules.login.waitForWeixinLogin({
        sessionKey,
        apiBaseUrl: modules.accounts.DEFAULT_BASE_URL,
        timeoutMs: 480_000,
      });
      if (result.connected && result.botToken && result.accountId) {
        const accountId = normalizeAccountId(result.accountId);
        modules.accounts.saveWeixinAccount(accountId, {
          token: result.botToken,
          baseUrl: result.baseUrl,
          userId: result.userId,
        });
        modules.accounts.registerWeixinAccountId(accountId);
        this.lastError = null;
        if (config) {
          await this.start({ ...config, enabled: true, accountId });
        }
        return {
          connected: true,
          accountId,
          message: result.message,
        };
      }
      return {
        connected: Boolean(result.connected),
        accountId: result.accountId ? normalizeAccountId(result.accountId) : undefined,
        message: result.message,
      };
    } catch (error) {
      this.lastError = error instanceof Error ? error.message : String(error);
      return { connected: false, message: `Login failed: ${this.lastError}` };
    }
  }

  async sendConversationReply(conversationId: string, text: string): Promise<boolean> {
    if (!this.account) return false;
    const modules = await this.loadPluginModules();
    const recipient = this.parseConversationId(conversationId)?.recipientId;
    if (!recipient) return false;
    const contextToken = modules.inbound.getContextToken(this.account.accountId, recipient);
    await modules.send.sendMessageWeixin({
      to: recipient,
      text,
      opts: {
        baseUrl: this.account.baseUrl,
        token: this.account.token,
        contextToken,
      },
    });
    this.lastOutboundAt = Date.now();
    return true;
  }

  private async pollLoop(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      const account = this.account;
      if (!account) return;
      try {
        const modules = await this.loadPluginModules();
        const response = await modules.api.getUpdates({
          baseUrl: account.baseUrl,
          token: account.token,
          get_updates_buf: account.getUpdatesBuf,
          timeoutMs: DEFAULT_LONG_POLL_MS,
        });
        if (signal.aborted) return;
        if (typeof response.get_updates_buf === 'string') {
          account.getUpdatesBuf = response.get_updates_buf;
          modules.syncBuf.saveGetUpdatesBuf(account.syncBufPath, response.get_updates_buf);
        }
        if (response.ret !== undefined && response.ret !== 0) {
          this.lastError = response.errmsg || `Weixin getUpdates returned ${response.ret}`;
          await this.sleep(3_000, signal);
          continue;
        }
        this.lastError = null;
        for (const msg of response.msgs ?? []) {
          if (signal.aborted) return;
          await this.handleRawMessage(msg, account, modules);
        }
      } catch (error) {
        if (signal.aborted) return;
        this.lastError = error instanceof Error ? error.message : String(error);
        console.warn('[NativeWeixinGateway] poll iteration failed:', error);
        await this.sleep(3_000, signal);
      }
    }
  }

  private async handleRawMessage(
    rawMessage: unknown,
    account: WeixinRuntimeAccount,
    modules: WeixinPluginModules,
  ): Promise<void> {
    const msg = asRecord(rawMessage);
    const messageId = getMessageStableId(msg, account.accountId);
    if (this.seenMessageIds.has(messageId)) return;
    this.seenMessageIds.add(messageId);
    if (this.seenMessageIds.size > 1000) {
      this.seenMessageIds = new Set([...this.seenMessageIds].slice(-500));
    }

    const context = modules.inbound.weixinMessageToMsgContext(rawMessage, account.accountId);
    if (!context.Body.trim() || !context.From) return;
    if (context.context_token) {
      modules.inbound.setContextToken(account.accountId, context.From, context.context_token);
    }

    const conversationId = this.buildConversationId(account.accountId, context.From);
    const message: IMMessage = {
      platform: 'weixin',
      messageId: context.MessageSid || messageId,
      conversationId,
      senderId: context.From,
      senderName: context.From,
      content: context.Body,
      chatType: 'direct',
      timestamp: context.Timestamp || Date.now(),
    };
    this.lastInboundAt = Date.now();
    await this.messageCallback?.(message, async (replyText) => {
      await this.sendConversationReply(conversationId, replyText);
    });
  }

  private buildConversationId(accountId: string, recipientId: string): string {
    return `${WEIXIN_CONVERSATION_PREFIX}:${accountId}:${recipientId}`;
  }

  private parseConversationId(conversationId: string): { accountId: string; recipientId: string } | null {
    const parts = conversationId.split(':');
    if (parts.length < 3 || parts[0] !== WEIXIN_CONVERSATION_PREFIX) return null;
    return {
      accountId: parts[1],
      recipientId: parts.slice(2).join(':'),
    };
  }

  private resolveAccountId(config: WeixinOpenClawConfig, modules: WeixinPluginModules): string | null {
    if (config.accountId?.trim()) return normalizeAccountId(config.accountId);
    const accountIds = modules.accounts.listWeixinAccountIds(this.buildOpenClawConfig(config));
    return accountIds[0] ? normalizeAccountId(accountIds[0]) : null;
  }

  private buildOpenClawConfig(config: WeixinOpenClawConfig, accountId?: string): Record<string, unknown> {
    return {
      channels: {
        'openclaw-weixin': {
          enabled: config.enabled,
          accountId: accountId || config.accountId || undefined,
        },
      },
    };
  }

  private async loadPluginModules(): Promise<WeixinPluginModules> {
    if (this.pluginModules) return this.pluginModules;
    const root = this.resolvePluginRoot();
    if (!root) {
      throw new Error('openclaw-weixin is not installed. Please install or start OpenClaw once, then retry WeChat login.');
    }
    const load = async <T>(relativePath: string): Promise<T> => {
      const filePath = path.join(root, relativePath);
      if (!fs.existsSync(filePath)) {
        throw new Error(`openclaw-weixin module is missing: ${relativePath}`);
      }
      return import(pathToFileURL(filePath).href) as Promise<T>;
    };
    this.pluginModules = {
      login: await load<WeixinPluginModules['login']>('dist/src/auth/login-qr.js'),
      accounts: await load<WeixinPluginModules['accounts']>('dist/src/auth/accounts.js'),
      api: await load<WeixinPluginModules['api']>('dist/src/api/api.js'),
      inbound: await load<WeixinPluginModules['inbound']>('dist/src/messaging/inbound.js'),
      send: await load<WeixinPluginModules['send']>('dist/src/messaging/send.js'),
      syncBuf: await load<WeixinPluginModules['syncBuf']>('dist/src/storage/sync-buf.js'),
    };
    return this.pluginModules;
  }

  private resolvePluginRoot(): string | null {
    const home = os.homedir();
    const candidates = [
      path.join(home, '.openclaw/npm/node_modules/@tencent-weixin/openclaw-weixin'),
      path.join(home, '.openclaw/extensions/openclaw-weixin'),
      path.join(process.cwd(), 'node_modules/@tencent-weixin/openclaw-weixin'),
    ];
    return candidates.find(candidate => fs.existsSync(path.join(candidate, 'dist/src/auth/login-qr.js'))) ?? null;
  }

  private sleep(ms: number, signal: AbortSignal): Promise<void> {
    return new Promise(resolve => {
      const timer = setTimeout(resolve, ms);
      signal.addEventListener('abort', () => {
        clearTimeout(timer);
        resolve();
      }, { once: true });
    });
  }
}

export type { NativeWeixinReply };

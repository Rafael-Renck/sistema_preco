/**
 * Sistema de Notificações Toast
 *
 * Tipos de notificações:
 * - success: Sucesso (verde)
 * - error: Erro (vermelho)
 * - warning: Aviso (amarelo/laranja)
 * - info: Informação (azul)
 *
 * Uso:
 * Toast.success('Busca realizada com sucesso!');
 * Toast.error('Erro ao carregar dados');
 * Toast.warning('Campo obrigatório');
 * Toast.info('Selecione uma UF');
 */

class Toast {
  static defaults = {
    duration: 4000,      // ms - 0 = permanente
    position: 'top-right', // top-right, top-left, top-center, bottom-right, bottom-left, bottom-center
    maxToasts: 5,        // Máximo de toasts simultâneos
    animationDuration: 300 // ms
  };

  static toasts = [];

  /**
   * Mostra notificação de sucesso
   */
  static success(message, options = {}) {
    return this.show(message, 'success', options);
  }

  /**
   * Mostra notificação de erro
   */
  static error(message, options = {}) {
    return this.show(message, 'error', options);
  }

  /**
   * Mostra notificação de aviso
   */
  static warning(message, options = {}) {
    return this.show(message, 'warning', options);
  }

  /**
   * Mostra notificação de informação
   */
  static info(message, options = {}) {
    return this.show(message, 'info', options);
  }

  /**
   * Mostra notificação genérica
   * @param {string} message - Texto da notificação
   * @param {string} type - success | error | warning | info
   * @param {object} options - Opções customizadas
   */
  static show(message, type = 'info', options = {}) {
    const config = { ...this.defaults, ...options };

    // Validações
    if (!message || typeof message !== 'string') {
      console.error('Toast: message deve ser uma string');
      return;
    }

    const validTypes = ['success', 'error', 'warning', 'info'];
    if (!validTypes.includes(type)) {
      type = 'info';
    }

    // Limita toasts simultâneos
    if (this.toasts.length >= config.maxToasts) {
      this.toasts[0].remove();
      this.toasts.shift();
    }

    // Cria container se não existir
    let container = document.getElementById('toast-container');
    if (!container) {
      container = document.createElement('div');
      container.id = 'toast-container';
      container.className = `toast-container toast-${config.position}`;
      document.body.appendChild(container);
    }

    // Cria elemento do toast
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = this._getHTML(message, type);
    container.appendChild(toast);

    // Anima entrada
    setTimeout(() => {
      toast.classList.add('toast-visible');
    }, 10);

    // Armazena referência
    const toastObj = {
      element: toast,
      timeout: null,
      remove: () => this._removeToast(toast, config.animationDuration)
    };

    this.toasts.push(toastObj);

    // Define timeout de auto-remoção
    if (config.duration > 0) {
      toastObj.timeout = setTimeout(() => {
        toastObj.remove();
      }, config.duration);
    }

    // Event listener para botão de fechar
    const closeBtn = toast.querySelector('.toast-close');
    if (closeBtn) {
      closeBtn.addEventListener('click', () => {
        if (toastObj.timeout) clearTimeout(toastObj.timeout);
        toastObj.remove();
      });
    }

    return toastObj;
  }

  /**
   * Remove todas as notificações
   */
  static clear() {
    this.toasts.forEach(t => t.remove());
    this.toasts = [];
  }

  /**
   * Retorna HTML do toast
   */
  static _getHTML(message, type) {
    const icons = {
      success: 'bi-check-circle-fill',
      error: 'bi-exclamation-circle-fill',
      warning: 'bi-exclamation-triangle-fill',
      info: 'bi-info-circle-fill'
    };

    const icon = icons[type] || icons.info;

    return `
      <div class="toast-content">
        <i class="bi ${icon} toast-icon"></i>
        <span class="toast-message">${this._escapeHTML(message)}</span>
      </div>
      <button class="toast-close" type="button" aria-label="Fechar">
        <i class="bi bi-x"></i>
      </button>
    `;
  }

  /**
   * Remove toast com animação
   */
  static _removeToast(element, duration) {
    element.classList.remove('toast-visible');
    setTimeout(() => {
      element.remove();
      this.toasts = this.toasts.filter(t => t.element !== element);
    }, duration);
  }

  /**
   * Escapa HTML para evitar XSS
   */
  static _escapeHTML(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }
}

// Exportar globalmente
window.Toast = Toast;

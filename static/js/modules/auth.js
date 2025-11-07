/**
 * Authentication Module
 * Handles password toggle, form validation, and auth-related interactions
 */

export class AuthModule {
  constructor() {
    this.init();
  }

  init() {
    this.setupPasswordToggle();
    this.setupFormValidation();
  }

  /**
   * Setup password visibility toggle
   */
  setupPasswordToggle() {
    document.querySelectorAll('[data-toggle-password]').forEach((btn) => {
      btn.addEventListener('click', (e) => {
        e.preventDefault();
        this.togglePasswordVisibility(btn);
      });
    });
  }

  /**
   * Toggle password field visibility
   * @param {HTMLElement} btn - Toggle button element
   */
  togglePasswordVisibility(btn) {
    const targetSelector = btn.getAttribute('data-target');
    if (!targetSelector) return;

    const input = document.querySelector(targetSelector);
    if (!input) return;

    const isPassword = input.type === 'password';
    input.type = isPassword ? 'text' : 'password';

    const icon = btn.querySelector('i');
    if (icon) {
      icon.classList.toggle('bi-eye');
      icon.classList.toggle('bi-eye-slash');
    }

    btn.setAttribute('aria-pressed', !isPassword);

    // Focus input for better UX
    try {
      input.focus({ preventScroll: true });
    } catch (err) {
      input.focus();
    }
  }

  /**
   * Setup basic form validation
   */
  setupFormValidation() {
    const forms = document.querySelectorAll('form[data-validate]');
    forms.forEach((form) => {
      form.addEventListener('submit', (e) => {
        if (!form.checkValidity()) {
          e.preventDefault();
          e.stopPropagation();
        }
        form.classList.add('was-validated');
      });
    });
  }

  /**
   * Show auth error message
   * @param {string} message - Error message
   * @param {string} title - Error title (optional)
   */
  showError(message, title = 'Erro') {
    const alertContainer = document.querySelector('[role="alert"]');
    if (!alertContainer) {
      console.error('Alert container not found');
      return;
    }

    alertContainer.innerHTML = `
      <i class="bi bi-exclamation-circle"></i>
      <div class="auth-alert-content">
        <div class="auth-alert-title">${title}</div>
        <div>${message}</div>
      </div>
    `;
    alertContainer.style.display = 'block';
  }

  /**
   * Clear auth error messages
   */
  clearErrors() {
    const alertContainer = document.querySelector('[role="alert"]');
    if (alertContainer) {
      alertContainer.style.display = 'none';
    }
  }
}

// Auto-initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => {
  new AuthModule();
});

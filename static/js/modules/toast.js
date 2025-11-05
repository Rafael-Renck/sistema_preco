/**
 * Sistema de notificações Toast
 */

const Toast = {
  show: (message, type = 'info', duration = 4000) => {
    const toastId = `toast-${Date.now()}`;
    const toast = document.createElement('div');
    toast.id = toastId;
    toast.className = `toast toast-${type}`;

    const iconMap = {
      success: 'check-circle',
      danger: 'exclamation-circle',
      warning: 'exclamation-triangle',
      info: 'info-circle'
    };

    toast.innerHTML = `
      <i class="bi bi-${iconMap[type] || 'info-circle'}"></i>
      <div class="toast-body">
        <div class="toast-message">${message}</div>
      </div>
    `;

    document.body.appendChild(toast);

    // Trigger animation
    setTimeout(() => toast.classList.add('show'), 10);

    if (duration > 0) {
      setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
      }, duration);
    }

    return {
      close: () => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
      }
    };
  },

  success: (message, duration = 3000) => Toast.show(message, 'success', duration),
  error: (message, duration = 4000) => Toast.show(message, 'danger', duration),
  warning: (message, duration = 3500) => Toast.show(message, 'warning', duration),
  info: (message, duration = 3000) => Toast.show(message, 'info', duration)
};

export default Toast;

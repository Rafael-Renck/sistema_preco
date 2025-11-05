/**
 * Gerenciador de Modais
 */

const Modal = {
  open: (modalId) => {
    const modal = document.getElementById(modalId);
    if (modal) {
      modal.classList.add('show');
      modal.style.display = 'flex';
      document.body.style.overflow = 'hidden';
    }
  },

  close: (modalId) => {
    const modal = document.getElementById(modalId);
    if (modal) {
      modal.classList.remove('show');
      modal.style.display = 'none';
      document.body.style.overflow = '';
    }
  },

  closeAll: () => {
    document.querySelectorAll('.modal.show').forEach(modal => {
      modal.classList.remove('show');
      modal.style.display = 'none';
    });
    document.body.style.overflow = '';
  }
};

// Event listeners para fechar modais
document.addEventListener('DOMContentLoaded', () => {
  // Fechar modal ao clicar no botão close
  document.querySelectorAll('.btn-close').forEach(btn => {
    btn.addEventListener('click', (e) => {
      const modal = e.target.closest('.modal');
      if (modal) {
        Modal.close(modal.id);
      }
    });
  });

  // Fechar modal ao clicar fora
  document.querySelectorAll('.modal').forEach(modal => {
    modal.addEventListener('click', (e) => {
      if (e.target === modal) {
        Modal.close(modal.id);
      }
    });
  });

  // Fechar modal ao clicar em botão com data-bs-dismiss
  document.querySelectorAll('[data-bs-dismiss="modal"]').forEach(btn => {
    btn.addEventListener('click', (e) => {
      const modal = e.target.closest('.modal');
      if (modal) {
        Modal.close(modal.id);
      }
    });
  });
});

export default Modal;

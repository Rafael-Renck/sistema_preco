/**
 * Gerenciador da Sidebar
 */

const Sidebar = {
  init: () => {
    const sidebar = document.querySelector('.modern-sidebar');
    const toggleBtn = document.querySelector('[data-toggle="sidebar"]');

    if (toggleBtn) {
      toggleBtn.addEventListener('click', () => {
        sidebar?.classList.toggle('open');
      });
    }

    // Fechar sidebar ao clicar fora em mobile
    document.addEventListener('click', (e) => {
      if (!e.target.closest('.modern-sidebar') && !e.target.closest('[data-toggle="sidebar"]')) {
        sidebar?.classList.remove('open');
      }
    });

    // Fechar sidebar ao mudar para desktop
    window.addEventListener('resize', () => {
      if (window.innerWidth >= 768) {
        sidebar?.classList.remove('open');
      }
    });
  },

  toggle: () => {
    document.querySelector('.modern-sidebar')?.classList.toggle('open');
  },

  close: () => {
    document.querySelector('.modern-sidebar')?.classList.remove('open');
  },

  open: () => {
    document.querySelector('.modern-sidebar')?.classList.add('open');
  }
};

// Inicializar ao carregar a página
document.addEventListener('DOMContentLoaded', () => Sidebar.init());

export default Sidebar;

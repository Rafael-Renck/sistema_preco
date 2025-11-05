/**
 * Script Principal
 * Inicializa todos os módulos e configurações gerais
 */

// Importar módulos
import Utils from './utils.js';
import API from './api.js';
import Toast from '../modules/toast.js';
import Modal from '../modules/modal.js';
import Sidebar from '../modules/sidebar.js';

// Expor globalmente
window.Utils = Utils;
window.API = API;
window.Toast = Toast;
window.Modal = Modal;
window.Sidebar = Sidebar;

/**
 * Inicialização global
 */
document.addEventListener('DOMContentLoaded', () => {
  // Inicializar Sidebar
  Sidebar.init();

  // Configurar handlers de formulário
  initFormHandlers();

  // Configurar busca global
  initGlobalSearch();

  // Configurar atalhos de teclado
  initKeyboardShortcuts();
});

/**
 * Inicializar handlers de formulário
 */
function initFormHandlers() {
  // Máscara de CPF
  document.querySelectorAll('[data-mask="cpf"]').forEach(input => {
    input.addEventListener('input', (e) => {
      e.target.value = Utils.maskCPF(e.target.value);
    });
  });

  // Máscara de Telefone
  document.querySelectorAll('[data-mask="phone"]').forEach(input => {
    input.addEventListener('input', (e) => {
      e.target.value = Utils.maskPhone(e.target.value);
    });
  });

  // Máscara de CEP
  document.querySelectorAll('[data-mask="cep"]').forEach(input => {
    input.addEventListener('input', (e) => {
      e.target.value = Utils.maskCEP(e.target.value);
    });
  });

  // Validação de email em tempo real
  document.querySelectorAll('[data-validate="email"]').forEach(input => {
    input.addEventListener('blur', (e) => {
      const isValid = Utils.isValidEmail(e.target.value);
      e.target.classList.toggle('is-invalid', !isValid && e.target.value !== '');
    });
  });
}

/**
 * Inicializar busca global
 */
function initGlobalSearch() {
  const searchInput = document.getElementById('globalSearch');

  if (searchInput) {
    // Debounce a busca para não sobrecarregar
    const handleSearch = Utils.debounce((e) => {
      const query = e.target.value.toLowerCase();

      if (query.length < 2) {
        // Limpar resultados
        return;
      }

      // Aqui você pode implementar a lógica de busca global
      console.log('Buscando:', query);
    }, 300);

    searchInput.addEventListener('input', handleSearch);

    // Atalho: "/" para focar a busca
    document.addEventListener('keydown', (e) => {
      if (e.key === '/' && !['INPUT', 'TEXTAREA'].includes(e.target.tagName)) {
        e.preventDefault();
        searchInput.focus();
      }
    });
  }
}

/**
 * Inicializar atalhos de teclado
 */
function initKeyboardShortcuts() {
  document.addEventListener('keydown', (e) => {
    // Ctrl/Cmd + S para salvar
    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
      e.preventDefault();
      const form = document.querySelector('form');
      if (form) {
        form.submit();
      }
    }

    // Esc para fechar modais
    if (e.key === 'Escape') {
      Modal.closeAll();
    }
  });
}

/**
 * Tratamento global de erros
 */
window.addEventListener('error', (event) => {
  console.error('Erro global:', event.error);
  Toast.error('Ocorreu um erro inesperado');
});

/**
 * Tratamento de erros de promise não capturados
 */
window.addEventListener('unhandledrejection', (event) => {
  console.error('Promise rejeitada:', event.reason);
  Toast.error('Erro ao processar requisição');
});

export { Utils, API, Toast, Modal, Sidebar };

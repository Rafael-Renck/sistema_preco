/**
 * Módulo de Autocomplete para Buscas de Insumos
 *
 * Funcionalidades:
 * - Autocomplete com API backend
 * - Histórico de buscas com localStorage
 * - Debounce para evitar sobrecarga
 * - Navegação por teclado (↑↓ Enter Escape)
 * - Exibição de últimas buscas ao focar
 */

class InsumoAutocomplete {
  constructor(options = {}) {
    this.options = {
      apiUrl: '/api/insumos/suggest',
      debounceDelay: 300,
      maxSuggestions: 10,
      maxHistoryItems: 10,
      minChars: 2,
      storageKey: 'insumo_search_history',
      ...options
    };

    this.fields = {};
    this.debounceTimers = {};
    this.selectedIndex = -1;
    this.currentDropdown = null;
  }

  /**
   * Inicializa autocomplete para um campo específico
   * @param {string} fieldId - ID do input element
   * @param {string} fieldName - Nome do campo (descricao, fabricante, tuss, tiss, anvisa)
   */
  init(fieldId, fieldName) {
    const input = document.getElementById(fieldId);
    if (!input) {
      console.warn(`Campo ${fieldId} não encontrado`);
      return;
    }

    this.fields[fieldId] = {
      element: input,
      fieldName: fieldName,
      dropdown: null,
      suggestions: []
    };

    // Event listeners
    input.addEventListener('input', (e) => this._onInput(e, fieldId));
    input.addEventListener('focus', (e) => this._onFocus(e, fieldId));
    input.addEventListener('blur', (e) => this._onBlur(e, fieldId));
    input.addEventListener('keydown', (e) => this._onKeydown(e, fieldId));

    console.log(`✅ Autocomplete inicializado para ${fieldId} (${fieldName})`);
  }

  /**
   * Handler para evento de input (digitação)
   */
  _onInput(event, fieldId) {
    const field = this.fields[fieldId];
    if (!field) return;

    const value = event.target.value.trim();

    // Limpa timer anterior
    if (this.debounceTimers[fieldId]) {
      clearTimeout(this.debounceTimers[fieldId]);
    }

    // Se vazio, mostra histórico
    if (value.length === 0) {
      this._showHistory(fieldId);
      return;
    }

    // Se muito curto, não busca
    if (value.length < this.options.minChars) {
      this._hideDropdown(fieldId);
      return;
    }

    // Debounce para não sobrecarregar API
    this.debounceTimers[fieldId] = setTimeout(() => {
      this._fetchSuggestions(fieldId, value);
    }, this.options.debounceDelay);
  }

  /**
   * Handler para evento de focus
   */
  _onFocus(event, fieldId) {
    const value = event.target.value.trim();

    // Se vazio, mostra histórico
    if (value.length === 0) {
      this._showHistory(fieldId);
    } else if (value.length >= this.options.minChars) {
      // Se tem valor, busca sugestões
      this._fetchSuggestions(fieldId, value);
    }
  }

  /**
   * Handler para evento de blur (deixar foco)
   */
  _onBlur(event, fieldId) {
    // Aguarda um pouco antes de esconder (para permitir click na sugestão)
    setTimeout(() => {
      this._hideDropdown(fieldId);
    }, 200);
  }

  /**
   * Handler para eventos de teclado
   */
  _onKeydown(event, fieldId) {
    const field = this.fields[fieldId];
    if (!field || !field.dropdown) return;

    const dropdown = field.dropdown;
    const items = dropdown.querySelectorAll('.autocomplete-item');

    switch (event.key) {
      case 'ArrowDown':
        event.preventDefault();
        this.selectedIndex = Math.min(this.selectedIndex + 1, items.length - 1);
        this._updateSelection(fieldId, items);
        break;

      case 'ArrowUp':
        event.preventDefault();
        this.selectedIndex = Math.max(this.selectedIndex - 1, -1);
        this._updateSelection(fieldId, items);
        break;

      case 'Enter':
        event.preventDefault();
        if (this.selectedIndex >= 0) {
          const item = items[this.selectedIndex];
          this._selectItem(fieldId, item.textContent);
        } else {
          // Submeter formulário se houver
          const form = field.element.closest('form');
          if (form) form.submit();
        }
        break;

      case 'Escape':
        event.preventDefault();
        this._hideDropdown(fieldId);
        break;
    }
  }

  /**
   * Busca sugestões na API
   */
  async _fetchSuggestions(fieldId, query) {
    const field = this.fields[fieldId];
    if (!field) return;

    try {
      const url = new URL(this.options.apiUrl, window.location.origin);
      url.searchParams.append('q', query);
      url.searchParams.append('field', field.fieldName);
      url.searchParams.append('limit', this.options.maxSuggestions);

      console.log(`🔍 Buscando sugestões: ${query}`);
      const response = await fetch(url.toString());

      if (!response.ok) {
        throw new Error(`API erro: ${response.statusText}`);
      }

      const data = await response.json();
      field.suggestions = data.suggestions || [];

      this._showDropdown(fieldId, field.suggestions);
    } catch (error) {
      console.error('❌ Erro ao buscar sugestões:', error);
      this._hideDropdown(fieldId);
    }
  }

  /**
   * Mostra histórico de buscas
   */
  _showHistory(fieldId) {
    const field = this.fields[fieldId];
    if (!field) return;

    const history = this._getHistory();
    if (history.length === 0) return;

    // Filtra apenas histórico do campo atual
    const fieldHistory = history.filter(h => h.field === field.fieldName);
    if (fieldHistory.length === 0) return;

    const suggestions = fieldHistory.map(h => h.value);
    this._showDropdown(fieldId, suggestions, 'history');
  }

  /**
   * Exibe dropdown com sugestões
   */
  _showDropdown(fieldId, suggestions, type = 'suggestions') {
    const field = this.fields[fieldId];
    if (!field) return;

    // Cria dropdown se não existir
    if (!field.dropdown) {
      field.dropdown = document.createElement('div');
      field.dropdown.className = 'autocomplete-dropdown';
      field.element.parentElement.appendChild(field.dropdown);
    }

    const dropdown = field.dropdown;
    this.selectedIndex = -1;

    if (suggestions.length === 0) {
      dropdown.innerHTML = '<div class="autocomplete-empty">Nenhuma sugestão</div>';
      dropdown.classList.add('visible');
      return;
    }

    const html = suggestions.map((s, i) => `
      <div class="autocomplete-item" data-index="${i}" title="${s}">
        <i class="bi ${type === 'history' ? 'bi-clock-history' : 'bi-search'}"></i>
        <span>${this._highlightMatch(s, field.element.value.trim())}</span>
      </div>
    `).join('');

    dropdown.innerHTML = html;
    dropdown.classList.add('visible');

    // Event listeners para items
    dropdown.querySelectorAll('.autocomplete-item').forEach((item) => {
      item.addEventListener('click', () => {
        this._selectItem(fieldId, item.textContent.trim());
      });
      item.addEventListener('mouseenter', () => {
        dropdown.querySelectorAll('.autocomplete-item').forEach(i => i.classList.remove('selected'));
        item.classList.add('selected');
        this.selectedIndex = parseInt(item.dataset.index);
      });
    });

    this.currentDropdown = dropdown;
  }

  /**
   * Esconde dropdown
   */
  _hideDropdown(fieldId) {
    const field = this.fields[fieldId];
    if (field && field.dropdown) {
      field.dropdown.classList.remove('visible');
      this.selectedIndex = -1;
    }
  }

  /**
   * Seleciona um item
   */
  _selectItem(fieldId, value) {
    const field = this.fields[fieldId];
    if (!field) return;

    field.element.value = value;
    this._addToHistory(field.fieldName, value);
    this._hideDropdown(fieldId);

    console.log(`✅ Selecionado: ${value}`);
  }

  /**
   * Atualiza seleção visual do teclado
   */
  _updateSelection(fieldId, items) {
    items.forEach((item, i) => {
      item.classList.toggle('selected', i === this.selectedIndex);
    });

    // Scroll para item selecionado
    if (this.selectedIndex >= 0 && items[this.selectedIndex]) {
      items[this.selectedIndex].scrollIntoView({ block: 'nearest' });
    }
  }

  /**
   * Destaca match no texto
   */
  _highlightMatch(text, query) {
    if (!query) return text;

    const regex = new RegExp(`(${query})`, 'gi');
    return text.replace(regex, '<strong>$1</strong>');
  }

  /**
   * Adiciona ao histórico
   */
  _addToHistory(field, value) {
    if (!value) return;

    let history = this._getHistory();

    // Remove se já existe
    history = history.filter(h => !(h.field === field && h.value === value));

    // Adiciona no início
    history.unshift({ field, value, timestamp: Date.now() });

    // Limita tamanho
    history = history.slice(0, this.options.maxHistoryItems);

    try {
      localStorage.setItem(this.options.storageKey, JSON.stringify(history));
    } catch (e) {
      console.warn('❌ Erro ao salvar histórico:', e);
    }
  }

  /**
   * Recupera histórico
   */
  _getHistory() {
    try {
      const stored = localStorage.getItem(this.options.storageKey);
      return stored ? JSON.parse(stored) : [];
    } catch (e) {
      console.warn('❌ Erro ao ler histórico:', e);
      return [];
    }
  }

  /**
   * Limpa histórico
   */
  clearHistory() {
    try {
      localStorage.removeItem(this.options.storageKey);
      console.log('✅ Histórico limpo');
    } catch (e) {
      console.warn('❌ Erro ao limpar histórico:', e);
    }
  }

  /**
   * Obtém histórico formatado
   */
  getHistory() {
    return this._getHistory();
  }
}

// Exportar para uso global
window.InsumoAutocomplete = InsumoAutocomplete;

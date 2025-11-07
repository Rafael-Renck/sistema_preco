/**
 * CONSULTA & COMPARAR - Module Principal
 * Interface moderna para análise comparativa de preços
 *
 * @author Claude (AI Assistant)
 * @version 2.0 - Refactoring Revolucionário
 *
 * Módulos:
 * - FilterManager: Gerenciamento de filtros e busca
 * - ComparadorTable: Renderização da tabela de comparação
 * - RadarAnalytics: Sistema de análise de oportunidades
 * - SimuladorCBHPM: Simulação de valores CBHPM
 * - UIController: Controle de interface e tabs
 */

// ═══════════════════════════════════════════════════════════════════════════
// UTILITY FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════

const Utils = {
  /**
   * Formata valor em BRL
   */
  formatBRL: (value) => {
    return new Intl.NumberFormat('pt-BR', {
      style: 'currency',
      currency: 'BRL',
    }).format(value);
  },

  /**
   * Formata percentual
   */
  formatPercent: (value) => {
    return `${(value * 100).toFixed(2)}%`;
  },

  /**
   * Escapa HTML
   */
  escapeHTML: (text) => {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  },

  /**
   * Delay em promise
   */
  delay: (ms) => new Promise(resolve => setTimeout(resolve, ms)),

  /**
   * Cria elemento com classes
   */
  createElement: (tag, classes = [], attrs = {}) => {
    const el = document.createElement(tag);
    if (classes.length) el.className = classes.join(' ');
    Object.entries(attrs).forEach(([key, value]) => {
      el.setAttribute(key, value);
    });
    return el;
  },

  /**
   * API Call com tratamento
   */
  fetchAPI: async (url, options = {}) => {
    try {
      const response = await fetch(url, options);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return await response.json();
    } catch (error) {
      console.error('API Error:', error);
      throw error;
    }
  }
};

// ═══════════════════════════════════════════════════════════════════════════
// FILTER MANAGER - Gerenciamento de Filtros
// ═══════════════════════════════════════════════════════════════════════════

class FilterManager {
  constructor() {
    this.selectedProcedimentos = new Set();
    this.selectedVersoes = new Set();
    this.selectedPrestadores = new Set();
    this.selectedTabela = null;
    this.selectedUF = null;

    this.init();
  }

  init() {
    this.setupEventListeners();
  }

  setupEventListeners() {
    // Select de Tabela - Atualizar filtros quando mudar
    const selectTabela = document.getElementById('selectTabela');
    if (selectTabela) {
      selectTabela.addEventListener('change', (e) => {
        this.selectedTabela = e.target.value;
        this.onTabelaChange();
      });
    }

    // Select de UF
    const selectUF = document.getElementById('selectUF');
    if (selectUF) {
      selectUF.addEventListener('change', (e) => {
        this.selectedUF = e.target.value;
      });
    }

    // Procedimento input
    const inputProc = document.getElementById('inputProcedimento');
    if (inputProc) {
      inputProc.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          this.addProcedimento(inputProc.value);
          inputProc.value = '';
        }
      });
    }

    // Botões de ação
    const btnComparar = document.getElementById('btnComparar');
    const btnLimpar = document.getElementById('btnLimpar');

    if (btnComparar) btnComparar.addEventListener('click', () => this.compare());
    if (btnLimpar) btnLimpar.addEventListener('click', () => this.clear());

    // Toggles de filtros avançados
    const toggleVersoes = document.getElementById('toggleVersoes');
    const togglePrestadores = document.getElementById('togglePrestadores');

    if (toggleVersoes) {
      toggleVersoes.addEventListener('click', () => this.toggleFilter('versoes'));
    }
    if (togglePrestadores) {
      togglePrestadores.addEventListener('click', () => this.toggleFilter('prestadores'));
    }
  }

  addProcedimento(codigo) {
    if (codigo.trim()) {
      this.selectedProcedimentos.add(codigo.trim());
      this.renderChips();
    }
  }

  removeProcedimento(codigo) {
    this.selectedProcedimentos.delete(codigo);
    this.renderChips();
  }

  renderChips() {
    const container = document.getElementById('procedimentosChips');
    if (!container) return;

    container.innerHTML = '';
    this.selectedProcedimentos.forEach(codigo => {
      const chip = Utils.createElement('div', ['cc-chip']);
      chip.innerHTML = `
        <span>${Utils.escapeHTML(codigo)}</span>
        <span class="cc-chip-remove">×</span>
      `;
      chip.querySelector('.cc-chip-remove').addEventListener('click', () => {
        this.removeProcedimento(codigo);
      });
      container.appendChild(chip);
    });
  }

  toggleFilter(type) {
    const container = document.getElementById(`filter${type.charAt(0).toUpperCase() + type.slice(1)}`);
    if (container) {
      container.style.display = container.style.display === 'none' ? 'block' : 'none';
    }
  }

  /**
   * Quando a tabela é selecionada, atualizar filtros e abrir prestadores
   */
  async onTabelaChange() {
    if (!this.selectedTabela) return;

    console.log('📊 Tabela selecionada:', this.selectedTabela);

    // 1. Determinar se é CBHPM ou Diárias/Taxas
    const isCBHPM = await this.isTableCBHPM(this.selectedTabela);

    console.log(`🎯 Tipo de tabela: ${isCBHPM ? 'CBHPM' : 'Diárias/Taxas'}`);

    // 2. Se for Diárias/Taxas, abrir filtro de Prestadores
    if (!isCBHPM) {
      // Abrir automaticamente
      const toggleBtn = document.getElementById('togglePrestadores');
      const filterContainer = document.getElementById('filterPrestadores');

      if (toggleBtn && filterContainer) {
        // Marcar como ativo
        toggleBtn.classList.add('active');
        filterContainer.style.display = 'block';

        // Carregar prestadores
        await this.loadPrestadores();
        console.log('✅ Filtro de Prestadores aberto!');
      }
    } else {
      // Se for CBHPM, abrir filtro de Versões
      const toggleBtn = document.getElementById('toggleVersoes');
      const filterContainer = document.getElementById('filterVersoes');

      if (toggleBtn && filterContainer) {
        toggleBtn.classList.add('active');
        filterContainer.style.display = 'block';

        // Carregar versões
        await this.loadVersoes();
        console.log('✅ Filtro de Versões aberto!');
      }
    }

    // 3. Atualizar placeholder do input de procedimentos
    const inputProc = document.getElementById('inputProcedimento');
    if (inputProc) {
      if (isCBHPM) {
        inputProc.placeholder = 'Código CBHPM (ex: 30401011)...';
      } else {
        inputProc.placeholder = 'Código DTP ou Serviço...';
      }
    }

    // 4. Limpar chips anteriores (opcional - comentado)
    // this.selectedProcedimentos.clear();
    // this.renderChips();
  }

  /**
   * Verificar se tabela é CBHPM ou DTP
   */
  async isTableCBHPM(tableId) {
    // Simulação - em produção, você faria uma chamada à API
    // Por enquanto, verificamos pelo tipo de tabela
    try {
      const response = await Utils.fetchAPI(`/api/tabela-info/${tableId}`);
      return response.tipo === 'cbhpm';
    } catch (error) {
      console.error('Erro ao verificar tipo de tabela:', error);
      // Fallback: assumir que é DTP se não conseguir verificar
      return false;
    }
  }

  /**
   * Carregar prestadores da tabela selecionada
   */
  async loadPrestadores() {
    if (!this.selectedTabela) return;

    try {
      const response = await Utils.fetchAPI(
        `/api/prestadores/${this.selectedTabela}?uf=${this.selectedUF || ''}`
      );

      const prestadores = response.prestadores || [];
      this.renderPrestadoresFilter(prestadores);

    } catch (error) {
      console.error('Erro ao carregar prestadores:', error);
    }
  }

  /**
   * Carregar versões da tabela CBHPM
   */
  async loadVersoes() {
    if (!this.selectedTabela) return;

    try {
      const response = await Utils.fetchAPI(
        `/api/versoes/${this.selectedTabela}`
      );

      const versoes = response.versoes || [];
      this.renderVersoesFilter(versoes);

    } catch (error) {
      console.error('Erro ao carregar versões:', error);
    }
  }

  /**
   * Renderizar checkboxes de prestadores
   */
  renderPrestadoresFilter(prestadores) {
    const container = document.getElementById('filterPrestadores');
    if (!container) return;

    if (prestadores.length === 0) {
      container.innerHTML = '<p class="cc-text-small" style="padding: 8px 0;">Nenhum prestador disponível</p>';
      return;
    }

    container.innerHTML = prestadores.map(prest => `
      <label style="display: flex; align-items: center; gap: 8px; padding: 6px 0; cursor: pointer; font-size: 12px;">
        <input
          type="checkbox"
          value="${Utils.escapeHTML(prest)}"
          data-type="prestador"
          style="cursor: pointer;"
        >
        <span>${Utils.escapeHTML(prest)}</span>
      </label>
    `).join('');

    // Adicionar event listeners aos checkboxes
    container.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
      checkbox.addEventListener('change', (e) => {
        // Aqui você pode adicionar lógica para armazenar seleções
        console.log('Prestador selecionado:', e.target.value);
      });
    });
  }

  /**
   * Renderizar checkboxes de versões
   */
  renderVersoesFilter(versoes) {
    const container = document.getElementById('filterVersoes');
    if (!container) return;

    if (versoes.length === 0) {
      container.innerHTML = '<p class="cc-text-small" style="padding: 8px 0;">Nenhuma versão disponível</p>';
      return;
    }

    container.innerHTML = versoes.map(versao => `
      <label style="display: flex; align-items: center; gap: 8px; padding: 6px 0; cursor: pointer; font-size: 12px;">
        <input
          type="checkbox"
          value="${Utils.escapeHTML(versao)}"
          data-type="versao"
          style="cursor: pointer;"
        >
        <span>${Utils.escapeHTML(versao)}</span>
      </label>
    `).join('');

    // Adicionar event listeners aos checkboxes
    container.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
      checkbox.addEventListener('change', (e) => {
        console.log('Versão selecionada:', e.target.value);
      });
    });
  }

  async compare() {
    console.log('Comparando...', {
      procedimentos: Array.from(this.selectedProcedimentos),
      uf: this.selectedUF,
      tabela: this.selectedTabela
    });

    // Aqui chamaria a API para buscar dados
    UI.showTab('comparador');
  }

  clear() {
    this.selectedProcedimentos.clear();
    this.selectedVersoes.clear();
    this.selectedPrestadores.clear();
    this.renderChips();
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// COMPARADOR TABLE - Renderização e Controle
// ═══════════════════════════════════════════════════════════════════════════

class ComparadorTable {
  constructor() {
    this.data = [];
    this.sorted = false;
  }

  /**
   * Renderiza tabela com dados
   */
  render(data) {
    this.data = data;
    const tbody = document.getElementById('comparadorBody');
    if (!tbody) return;

    if (!data || data.length === 0) {
      tbody.innerHTML = `
        <tr>
          <td colspan="8" style="text-align: center; padding: 48px 24px;">
            <div class="cc-empty-state">
              <div class="cc-empty-state-icon">📭</div>
              <div class="cc-empty-state-title">Nenhum resultado encontrado</div>
              <div class="cc-empty-state-text">Tente ajustar seus filtros</div>
            </div>
          </td>
        </tr>
      `;
      return;
    }

    tbody.innerHTML = data.map((item, idx) => `
      <tr>
        <td><strong>${Utils.escapeHTML(item.codigo)}</strong></td>
        <td>${Utils.escapeHTML(item.descricao.substring(0, 40))}</td>
        <td>
          ${item.rol ? '<span class="cc-badge cc-badge--success">SIM</span>' : '<span class="cc-badge cc-badge--danger">NÃO</span>'}
        </td>
        <td><span class="cc-value-low">${Utils.formatBRL(item.minimo)}</span></td>
        <td><span class="cc-value-mid">${Utils.formatBRL(item.media)}</span></td>
        <td><span class="cc-value-high">${Utils.formatBRL(item.maximo)}</span></td>
        <td>
          <strong class="cc-text-primary">${((item.amplitude) * 100).toFixed(1)}%</strong>
        </td>
        <td>
          <button class="cc-button cc-button--secondary" style="width: auto; padding: 4px 8px; font-size: 11px;">
            Detalhe
          </button>
        </td>
      </tr>
    `).join('');

    // Update stats
    this.updateStats(data);
  }

  /**
   * Atualiza estatísticas
   */
  updateStats(data) {
    const totalAmplitude = data.reduce((sum, item) => sum + item.amplitude, 0) / data.length;
    const totalEconomia = data.reduce((sum, item) => sum + (item.maximo - item.minimo), 0);

    document.getElementById('statResultados').textContent = data.length;
    document.getElementById('statAmplitudeValue').textContent = (totalAmplitude * 100).toFixed(1);
    document.getElementById('statEconomiaValue').textContent = totalEconomia.toLocaleString('pt-BR', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
  }

  /**
   * Exporta dados em CSV
   */
  exportCSV() {
    if (!this.data.length) {
      alert('Nenhum dado para exportar');
      return;
    }

    const csv = [
      ['Código', 'Descrição', 'ROL', 'Mínimo', 'Média', 'Máximo', 'Amplitude'],
      ...this.data.map(item => [
        item.codigo,
        item.descricao,
        item.rol ? 'Sim' : 'Não',
        item.minimo,
        item.media,
        item.maximo,
        (item.amplitude * 100).toFixed(1) + '%'
      ])
    ];

    const csvContent = csv.map(row => row.map(cell => `"${cell}"`).join(',')).join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `comparacao-${new Date().toISOString().split('T')[0]}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  }

  /**
   * Copia tabela para clipboard
   */
  copyToClipboard() {
    if (!this.data.length) {
      alert('Nenhum dado para copiar');
      return;
    }

    const text = [
      'Código\tDescrição\tROL\tMínimo\tMédia\tMáximo\tAmplitude',
      ...this.data.map(item =>
        `${item.codigo}\t${item.descricao}\t${item.rol ? 'Sim' : 'Não'}\t${item.minimo}\t${item.media}\t${item.maximo}\t${(item.amplitude * 100).toFixed(1)}%`
      )
    ].join('\n');

    navigator.clipboard.writeText(text).then(() => {
      alert('Copiado para clipboard!');
    });
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// RADAR ANALYTICS - Análise de Oportunidades
// ═══════════════════════════════════════════════════════════════════════════

class RadarAnalytics {
  constructor(comparador) {
    this.comparador = comparador;
  }

  /**
   * Analisa dados e identifica oportunidades
   */
  analyze(data) {
    return data
      .map(item => ({
        ...item,
        amplitude: (item.maximo - item.minimo) / item.media,
        potencial: (item.media - item.minimo) / item.media,
        spread: ((item.maximo - item.minimo) / item.media) * 100
      }))
      .sort((a, b) => b.amplitude - a.amplitude)
      .slice(0, 10);
  }

  /**
   * Renderiza grid de oportunidades
   */
  render(data) {
    const grid = document.getElementById('radarGrid');
    if (!grid) return;

    const opportunities = this.analyze(data);

    grid.innerHTML = opportunities.map(item => `
      <div class="cc-data-card">
        <div class="cc-data-card-code">${Utils.escapeHTML(item.codigo)}</div>
        <div class="cc-data-card-title">${Utils.escapeHTML(item.descricao)}</div>

        <div class="cc-data-card-values">
          <div class="cc-data-card-value">
            <div class="cc-data-card-value-label">Mín</div>
            <div class="cc-data-card-value-amount">${Utils.formatBRL(item.minimo)}</div>
          </div>
          <div class="cc-data-card-value">
            <div class="cc-data-card-value-label">Méd</div>
            <div class="cc-data-card-value-amount">${Utils.formatBRL(item.media)}</div>
          </div>
          <div class="cc-data-card-value">
            <div class="cc-data-card-value-label">Máx</div>
            <div class="cc-data-card-value-amount">${Utils.formatBRL(item.maximo)}</div>
          </div>
        </div>

        <div style="display: flex; gap: 6px; flex-wrap: wrap; margin-top: 10px;">
          <span class="cc-badge">Spread: ${item.spread.toFixed(1)}%</span>
          <span class="cc-badge cc-badge--warning">Amplitude: ${(item.amplitude * 100).toFixed(1)}%</span>
        </div>

        <button class="cc-button cc-mt-12" style="font-size: 11px;">
          🔍 Detalhar
        </button>
      </div>
    `).join('');
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// SIMULADOR CBHPM - Cálculos e Simulação
// ═══════════════════════════════════════════════════════════════════════════

class SimuladorCBHPM {
  constructor() {
    this.setupEventListeners();
  }

  setupEventListeners() {
    // Inputs do simulador
    const inputs = ['simCodigo', 'simUCO', 'simPorte', 'simPorteAN'];
    inputs.forEach(id => {
      const input = document.getElementById(id);
      if (input) {
        input.addEventListener('change', () => this.simulate());
        input.addEventListener('keyup', () => this.simulate());
      }
    });
  }

  /**
   * Realiza simulação
   */
  simulate() {
    const codigo = document.getElementById('simCodigo')?.value;
    const uco = parseFloat(document.getElementById('simUCO')?.value || 0);
    const porte = parseFloat(document.getElementById('simPorte')?.value || 0);
    const porteAN = parseFloat(document.getElementById('simPorteAN')?.value || 0);

    if (!codigo || !uco) return;

    // Simples cálculo para demo
    const base = uco * 10; // Multiplicador fictício
    const ajustePorte = base * (porte / 100);
    const ajustePorteAN = base * (porteAN / 100);
    const total = base + ajustePorte + ajustePorteAN;

    document.getElementById('simResult').textContent = Utils.formatBRL(total);
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// UI CONTROLLER - Controle de Interface
// ═══════════════════════════════════════════════════════════════════════════

class UIController {
  constructor() {
    this.init();
  }

  init() {
    this.setupTabNavigation();
    this.setupButtonActions();
  }

  /**
   * Configura navegação de abas
   */
  setupTabNavigation() {
    const tabs = document.querySelectorAll('.cc-tab');
    tabs.forEach(tab => {
      tab.addEventListener('click', () => {
        const tabName = tab.getAttribute('data-tab');
        this.showTab(tabName);
      });
    });
  }

  /**
   * Mostra aba específica
   */
  showTab(tabName) {
    // Hide all panels
    document.querySelectorAll('.cc-tab-panel').forEach(panel => {
      panel.style.display = 'none';
    });

    // Deactivate all tabs
    document.querySelectorAll('.cc-tab').forEach(tab => {
      tab.classList.remove('active');
    });

    // Show selected panel
    const panel = document.getElementById(`tab-${tabName}`);
    if (panel) panel.style.display = 'block';

    // Activate tab
    const tab = document.querySelector(`[data-tab="${tabName}"]`);
    if (tab) tab.classList.add('active');
  }

  /**
   * Configura botões de ação
   */
  setupButtonActions() {
    const btnExportarCSV = document.getElementById('btnExportarCSV');
    const btnCopiar = document.getElementById('btnCopiar');
    const btnRadar = document.getElementById('btnRadar');

    if (btnExportarCSV) {
      btnExportarCSV.addEventListener('click', () => comparador.exportCSV());
    }

    if (btnCopiar) {
      btnCopiar.addEventListener('click', () => comparador.copyToClipboard());
    }

    if (btnRadar) {
      btnRadar.addEventListener('click', () => {
        this.showTab('radar');
        radar.render(comparador.data);
      });
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// INICIALIZAÇÃO
// ═══════════════════════════════════════════════════════════════════════════

// Instanciar módulos
const filterManager = new FilterManager();
const comparador = new ComparadorTable();
const radar = new RadarAnalytics(comparador);
const simulador = new SimuladorCBHPM();
const UI = new UIController();

// Exportar para uso global se necessário
window.consultaComparar = {
  filterManager,
  comparador,
  radar,
  simulador,
  Utils
};

console.log('✅ Consulta & Comparar - Nova Interface Carregada');

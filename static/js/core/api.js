/**
 * Módulo para requisições HTTP
 */

const API = {
  /**
   * Requisição GET
   */
  get: async (url, options = {}) => {
    try {
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
          ...options.headers
        },
        ...options
      });
      return await API.handleResponse(response);
    } catch (error) {
      console.error('Erro na requisição GET:', error);
      throw error;
    }
  },

  /**
   * Requisição POST
   */
  post: async (url, data = {}, options = {}) => {
    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...options.headers
        },
        body: JSON.stringify(data),
        ...options
      });
      return await API.handleResponse(response);
    } catch (error) {
      console.error('Erro na requisição POST:', error);
      throw error;
    }
  },

  /**
   * Requisição PUT
   */
  put: async (url, data = {}, options = {}) => {
    try {
      const response = await fetch(url, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          ...options.headers
        },
        body: JSON.stringify(data),
        ...options
      });
      return await API.handleResponse(response);
    } catch (error) {
      console.error('Erro na requisição PUT:', error);
      throw error;
    }
  },

  /**
   * Requisição DELETE
   */
  delete: async (url, options = {}) => {
    try {
      const response = await fetch(url, {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
          ...options.headers
        },
        ...options
      });
      return await API.handleResponse(response);
    } catch (error) {
      console.error('Erro na requisição DELETE:', error);
      throw error;
    }
  },

  /**
   * Requisição PATCH
   */
  patch: async (url, data = {}, options = {}) => {
    try {
      const response = await fetch(url, {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
          ...options.headers
        },
        body: JSON.stringify(data),
        ...options
      });
      return await API.handleResponse(response);
    } catch (error) {
      console.error('Erro na requisição PATCH:', error);
      throw error;
    }
  },

  /**
   * Processar resposta
   */
  handleResponse: async (response) => {
    const contentType = response.headers.get('content-type');
    let data;

    if (contentType && contentType.includes('application/json')) {
      data = await response.json();
    } else {
      data = await response.text();
    }

    if (!response.ok) {
      const error = new Error(data.message || 'Erro na requisição');
      error.status = response.status;
      error.data = data;
      throw error;
    }

    return data;
  },

  /**
   * Upload de arquivo
   */
  upload: async (url, formData, onProgress = null) => {
    try {
      const xhr = new XMLHttpRequest();

      if (onProgress) {
        xhr.upload.addEventListener('progress', (e) => {
          if (e.lengthComputable) {
            const percentComplete = (e.loaded / e.total) * 100;
            onProgress(percentComplete);
          }
        });
      }

      return new Promise((resolve, reject) => {
        xhr.addEventListener('load', () => {
          if (xhr.status >= 200 && xhr.status < 300) {
            try {
              const response = JSON.parse(xhr.responseText);
              resolve(response);
            } catch {
              resolve(xhr.responseText);
            }
          } else {
            reject(new Error(`Erro ${xhr.status}: ${xhr.statusText}`));
          }
        });

        xhr.addEventListener('error', () => {
          reject(new Error('Erro na requisição'));
        });

        xhr.open('POST', url);
        xhr.send(formData);
      });
    } catch (error) {
      console.error('Erro no upload:', error);
      throw error;
    }
  }
};

export default API;

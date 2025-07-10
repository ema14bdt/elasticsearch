const {createApp} = Vue

createApp({
	data() {
		return {
			// Estado de la aplicación
			selectedFile: null,
			indexName: '',
			indices: [],
			selectedIndex: '',
			searchQuery: '',
			searchSize: 10,
			searchResults: [],
			searchMetrics: null,
			lastSearchQuery: '',
			searchHistory: [],

			// Identificador de sesión
			sessionId: null,

			// Estados de carga
			uploading: false,
			searching: false,

			// Alertas
			alert: {
				message: '',
				type: 'success',
			},

			// Chart.js instance
			chart: null,
		}
	},

	async mounted() {
		console.log('🚀 Aplicación Vue.js iniciada')
		this.initializeSession()
		await this.loadIndices()
	},

	methods: {
		/**
		 * Maneja la selección de archivo CSV
		 */
		handleFileSelect(event) {
			const file = event.target.files[0]
			if (file && file.type === 'text/csv') {
				this.selectedFile = file
				console.log('📁 Archivo seleccionado:', file.name)

				// Auto-generar nombre de índice basado en el archivo
				const baseName = file.name.replace('.csv', '').toLowerCase()
				this.indexName = baseName.replace(/[^a-z0-9_-]/g, '_')

				this.showAlert('Archivo CSV seleccionado correctamente', 'success')
			} else {
				this.showAlert('Por favor selecciona un archivo CSV válido', 'error')
			}
		},

		/**
		 * Genera un UUID, usando crypto.randomUUID si está disponible, o un fallback.
		 */
		generateUUID() {
			if (typeof crypto !== 'undefined' && crypto.randomUUID) {
				return crypto.randomUUID()
			} else {
				// Fallback para navegadores antiguos o entornos sin crypto.randomUUID
				return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
					var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8)
					return v.toString(16)
				})
			}
		},

		/**
		 * Inicializa la sesión del usuario
		 */
		initializeSession() {
			console.log('DEBUG: Entrando a initializeSession')
			let sessionId = sessionStorage.getItem('userSessionId')
			console.log('DEBUG: sessionId de sessionStorage:', sessionId)
			if (!sessionId) {
				sessionId = this.generateUUID()
				sessionStorage.setItem('userSessionId', sessionId)
				console.log('🔑 Nueva sesión iniciada:', sessionId)
			} else {
				console.log('👍 Sesión existente:', sessionId)
			}
			this.sessionId = sessionId
			console.log('DEBUG: this.sessionId final:', this.sessionId)
		},

		/**
		 * Sube el archivo CSV al servidor
		 */
		async uploadCSV() {
			if (!this.sessionId) {
				this.showAlert('Error: No se pudo iniciar la sesión. Intenta recargar la página.', 'error')
				return
			}

			if (!this.selectedFile || !this.indexName) {
				this.showAlert('Selecciona un archivo y especifica un nombre de índice', 'error')
				return
			}

			this.uploading = true
			console.log('📤 Iniciando carga de CSV...')

			try {
				const formData = new FormData()
				formData.append('file', this.selectedFile)
				formData.append('index_name', this.indexName)
				formData.append('session_id', this.sessionId)

				const response = await axios.post('/api/upload-csv', formData, {
					headers: {
						'Content-Type': 'multipart/form-data',
					},
				})

				console.log('✅ CSV cargado exitosamente:', response.data)

				this.showAlert(
					`CSV cargado exitosamente. ${response.data.stats.success_count} documentos indexados en ${response.data.stats.indexing_time_seconds}s`,
					'success'
				)

				// Limpiar formulario
				this.selectedFile = null
				this.indexName = ''
				this.$refs.fileInput.value = ''

				// Recargar índices
				await this.loadIndices()
			} catch (error) {
				console.error('❌ Error cargando CSV:', error)
				this.showAlert(error.response?.data?.detail || 'Error cargando el archivo CSV', 'error')
			} finally {
				this.uploading = false
			}
		},

		/**
		 * Carga la lista de índices disponibles
		 */
		async loadIndices() {
			console.log('📋 Cargando índices...')
			if (!this.sessionId) return // No cargar si no hay sesión

			try {
				const response = await axios.get(`/api/indices?session_id=${this.sessionId}`)
				this.indices = response.data.indices.map(index => ({
					...index,
					full_name: index.name, // Guardar el nombre completo
					name: index.name.replace(/^temp-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}-/, '') // Nombre para mostrar
				}))
				console.log('📊 Índices cargados:', this.indices.length)

				// Auto-seleccionar el primer índice si hay alguno
				if (this.indices.length > 0 && !this.selectedIndex) {
					this.selectedIndex = this.indices[0].full_name // Usar el nombre completo para la selección interna
				}
			} catch (error) {
				console.error('❌ Error cargando índices:', error)
				this.showAlert('Error cargando la lista de índices', 'error')
			}
		},

		/**
		 * Selecciona un índice para búsqueda
		 */
		selectIndex(index) {
			this.selectedIndex = index.full_name
			console.log('🎯 Índice seleccionado:', index.name)
		},

		/**
		 * Realiza una búsqueda en Elasticsearch
		 */
		async performSearch() {
			if (!this.sessionId) {
				this.showAlert('Error: No se pudo iniciar la sesión. Intenta recargar la página.', 'error')
				return
			}

			if (!this.selectedIndex || !this.searchQuery.trim()) {
				this.showAlert('Selecciona un índice y escribe una consulta', 'error')
				return
			}

			this.searching = true
			this.lastSearchQuery = this.searchQuery
			console.log('🔍 Realizando búsqueda:', this.searchQuery)

			try {
				const textColumns = this.indices
                    .find(i => i.full_name === this.selectedIndex)
                    ?.columns.filter(c => c.type === 'text').map(c => c.name) || []

				const response = await fetch('/api/search', {
					method: 'POST',
					headers: {
						'Content-Type': 'application/json'
					},
					body: JSON.stringify({
						index_name: this.selectedIndex,
						query: this.searchQuery,
						size: this.searchSize,
                        agg_fields: textColumns,
						session_id: this.sessionId
					})
				})

				if (!response.ok) {
					const errorData = await response.json()
					throw new Error(errorData.detail || 'Error realizando la búsqueda')
				}

				const data = await response.json()
				this.searchResults = data.results
				this.searchMetrics = data

				// Agregar al historial para métricas
				this.searchHistory.push({
					query: this.searchQuery,
					time: data.search_time_seconds,
					results: data.total_results,
					timestamp: new Date(),
				})

				// Mantener solo los últimos 10 registros
				if (this.searchHistory.length > 10) {
					this.searchHistory.shift()
				}

				console.log('✅ Búsqueda completada:', data)

				this.showAlert(`Búsqueda completada: ${data.total_results} resultados en ${data.search_time_seconds}s`, 'success')

				// Actualizar gráfico
				this.$nextTick(() => {
					this.updateChart()
				})
			} catch (error) {
				console.error('❌ Error en búsqueda:', error)
				this.showAlert(error.message || 'Error realizando la búsqueda', 'error')
			} finally {
				this.searching = false
			}
		},

		/**
		 * Actualiza el gráfico de métricas
		 */
		updateChart() {
			if (this.searchHistory.length < 1) return  // Mostrar gráfico desde la primera búsqueda

			const ctx = this.$refs.metricsChart?.getContext('2d')
			if (!ctx) return

			// Destruir gráfico anterior si existe
			if (this.chart) {
				this.chart.destroy()
			}

			this.chart = new Chart(ctx, {
				type: 'line',
				data: {
					labels: this.searchHistory.map((_, i) => `#${i + 1}`),
					datasets: [
						{
							label: 'Tiempo de búsqueda (s)',
							data: this.searchHistory.map(h => h.time),
							borderColor: '#667eea',
							backgroundColor: 'rgba(102, 126, 234, 0.1)',
							tension: 0.4,
							fill: true,
						},
					],
				},
				options: {
					responsive: true,
					plugins: {
						legend: {
							display: false,
						},
					},
					scales: {
						y: {
							beginAtZero: true,
							title: {
								display: true,
								text: 'Segundos',
							},
						},
					},
				},
			})
		},

		/**
		 * Muestra una alerta al usuario
		 */
		showAlert(message, type = 'success') {
			this.alert.message = message
			this.alert.type = type

			// Auto-ocultar después de 5 segundos
			setTimeout(() => {
				this.alert.message = ''
			}, 5000)
		},

		/**
		 * Formatea el tamaño de archivo
		 */
		formatFileSize(bytes) {
			if (bytes === 0) return '0 Bytes'
			const k = 1024
			const sizes = ['Bytes', 'KB', 'MB', 'GB']
			const i = Math.floor(Math.log(bytes) / Math.log(k))
			return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
		},
	},
}).mount('#app')

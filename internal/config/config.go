package config

import (
	"log"
)

// Delta represents the configuration for the Delta Exchange websocket
type Delta struct {
	Enabled      bool     `mapstructure:"enabled"`
	URL          string   `mapstructure:"url"`
	Channels     []string `mapstructure:"channels"`
	ProductIDs   []string `mapstructure:"product_ids"`
	ReconnectMax int      `mapstructure:"reconnect_max"`
}

// WebsocketConfig holds WebSocket server configuration
type WebsocketConfig struct {
	ReadBufferSize  int      `yaml:"readBufferSize"`
	WriteBufferSize int      `yaml:"writeBufferSize"`
	CheckOrigin     bool     `yaml:"checkOrigin"`
	AllowedOrigins  []string `yaml:"allowedOrigins"`
}

// Config represents the configuration for the websocket service
type Config struct {
	// Service configuration
	ServiceName string `mapstructure:"service_name"`
	Environment string `mapstructure:"environment"`
	LogLevel    string `mapstructure:"log_level"`
	HTTPPort    int    `mapstructure:"http_port"`
	GRPCPort    int    `mapstructure:"grpc_port"`

	// Websocket configuration
	Websocket WebsocketConfig `yaml:"websocket"`

	// Security configuration
	Security struct {
		CORSEnabled        bool   `mapstructure:"cors_enabled"`
		CORSAllowedOrigins string `mapstructure:"cors_allowed_origins"`
		RateLimitEnabled   bool   `mapstructure:"rate_limit_enabled"`
		RateLimitRequests  int    `mapstructure:"rate_limit_requests"`
		RateLimitDuration  int    `mapstructure:"rate_limit_duration"`
	} `mapstructure:"security"`

	// Delta Exchange configuration
	Delta Delta `mapstructure:"delta"`

	// Metrics configuration
	Metrics struct {
		Enabled  bool   `mapstructure:"enabled"`
		Endpoint string `mapstructure:"endpoint"`
	} `mapstructure:"metrics"`
}

// LoadConfig loads the configuration from the config file
func LoadConfig(serviceName string) (*Config, error) {
	// Set default configuration values
	config := &Config{
		ServiceName: serviceName,
		Environment: "local",
		LogLevel:    "info",
		HTTPPort:    8083,
		GRPCPort:    9093,
		Delta: Delta{
			Enabled:      true,
			URL:          "wss://socket.india.delta.exchange",
			Channels:     []string{"v2/ticker"},
			ProductIDs:   []string{"BTCUSD"},
			ReconnectMax: 5,
		},
	}

	// // Use the server-utils config loader
	// v, err := sc.LoadConfig(serviceName)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to load config: %w", err)
	// }

	// // Unmarshal the configuration
	// if err := v.Unmarshal(config); err != nil {
	// 	return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	// }

	// Log the configuration
	log.Printf("Loaded configuration for %s in %s environment", config.ServiceName, config.Environment)

	return config, nil
}

// GetCORSAllowedOrigins returns the allowed origins for CORS
func (c *Config) GetCORSAllowedOrigins() []string {
	if len(c.Websocket.AllowedOrigins) == 0 {
		return []string{"*"} // Allow all origins if none specified
	}
	return c.Websocket.AllowedOrigins
}

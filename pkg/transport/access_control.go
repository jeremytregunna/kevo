package transport

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrAccessDenied indicates the replica is not authorized
	ErrAccessDenied = errors.New("access denied")
	
	// ErrAuthenticationFailed indicates authentication failure
	ErrAuthenticationFailed = errors.New("authentication failed")
	
	// ErrInvalidToken indicates an invalid or expired token
	ErrInvalidToken = errors.New("invalid or expired token")
)

// AuthMethod defines authentication methods for replicas
type AuthMethod string

const (
	// AuthNone means no authentication required (not recommended for production)
	AuthNone AuthMethod = "none"
	
	// AuthToken uses a pre-shared token for authentication
	AuthToken AuthMethod = "token"
)

// AccessLevel defines permission levels for replicas
type AccessLevel int

const (
	// AccessNone has no permissions
	AccessNone AccessLevel = iota
	
	// AccessReadOnly can only read from the primary
	AccessReadOnly
	
	// AccessReadWrite can read and receive updates from the primary
	AccessReadWrite
	
	// AccessAdmin has full control including management operations
	AccessAdmin
)

// ReplicaCredentials contains authentication information for a replica
type ReplicaCredentials struct {
	ReplicaID     string
	AuthMethod    AuthMethod
	Token         string      // Token for authentication (in a production system, this would be hashed)
	AccessLevel   AccessLevel
	ExpiresAt     time.Time   // Token expiration time (zero means no expiration)
}

// AccessController manages authentication and authorization for replicas
type AccessController struct {
	mu          sync.RWMutex
	credentials map[string]*ReplicaCredentials // Map of replicaID -> credentials
	enabled     bool
	defaultAuth AuthMethod
}

// NewAccessController creates a new access controller
func NewAccessController(enabled bool, defaultAuth AuthMethod) *AccessController {
	return &AccessController{
		credentials: make(map[string]*ReplicaCredentials),
		enabled:     enabled,
		defaultAuth: defaultAuth,
	}
}

// IsEnabled returns whether access control is enabled
func (ac *AccessController) IsEnabled() bool {
	return ac.enabled
}

// DefaultAuthMethod returns the default authentication method
func (ac *AccessController) DefaultAuthMethod() AuthMethod {
	return ac.defaultAuth
}

// RegisterReplica registers a new replica with credentials
func (ac *AccessController) RegisterReplica(creds *ReplicaCredentials) error {
	if !ac.enabled {
		// If access control is disabled, we still register the replica but don't enforce controls
		creds.AccessLevel = AccessAdmin
	}
	
	ac.mu.Lock()
	defer ac.mu.Unlock()
	
	// Store credentials (in a real system, we'd hash tokens here)
	ac.credentials[creds.ReplicaID] = creds
	return nil
}

// RemoveReplica removes a replica's credentials
func (ac *AccessController) RemoveReplica(replicaID string) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	
	delete(ac.credentials, replicaID)
}

// AuthenticateReplica authenticates a replica based on the provided credentials
func (ac *AccessController) AuthenticateReplica(replicaID, token string) error {
	if !ac.enabled {
		return nil // Authentication disabled
	}
	
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	
	creds, exists := ac.credentials[replicaID]
	if !exists {
		return ErrAccessDenied
	}
	
	// Check if credentials are expired
	if !creds.ExpiresAt.IsZero() && time.Now().After(creds.ExpiresAt) {
		return ErrInvalidToken
	}
	
	// Authenticate based on method
	switch creds.AuthMethod {
	case AuthNone:
		return nil // No authentication required
		
	case AuthToken:
		// In a real system, we'd compare hashed tokens
		if token != creds.Token {
			return ErrAuthenticationFailed
		}
		return nil
		
	default:
		return ErrAuthenticationFailed
	}
}

// AuthorizeReplicaAction checks if a replica has permission for an action
func (ac *AccessController) AuthorizeReplicaAction(replicaID string, requiredLevel AccessLevel) error {
	if !ac.enabled {
		return nil // Authorization disabled
	}
	
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	
	creds, exists := ac.credentials[replicaID]
	if !exists {
		return ErrAccessDenied
	}
	
	// Check permissions
	if creds.AccessLevel < requiredLevel {
		return ErrAccessDenied
	}
	
	return nil
}

// GetReplicaAccessLevel returns the access level for a replica
func (ac *AccessController) GetReplicaAccessLevel(replicaID string) (AccessLevel, error) {
	if !ac.enabled {
		return AccessAdmin, nil // If disabled, return highest access level
	}
	
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	
	creds, exists := ac.credentials[replicaID]
	if !exists {
		return AccessNone, ErrAccessDenied
	}
	
	return creds.AccessLevel, nil
}

// SetReplicaAccessLevel updates the access level for a replica
func (ac *AccessController) SetReplicaAccessLevel(replicaID string, level AccessLevel) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	
	creds, exists := ac.credentials[replicaID]
	if !exists {
		return ErrAccessDenied
	}
	
	creds.AccessLevel = level
	return nil
}
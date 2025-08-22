// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Encryption utilities for snapshot data


use thiserror::Error;

/// Simple XOR-based encryption for demonstration
/// In production, use proper encryption libraries like AES-GCM
#[derive(Debug, Clone)]
pub struct EncryptionEngine {
    key: Vec<u8>,
}

impl EncryptionEngine {
    /// Create new encryption engine with key
    pub fn new(key: Vec<u8>) -> Self {
        Self { key }
    }

    /// Create encryption engine with random key
    pub fn new_random() -> Self {
        let key = (0..32).map(|_| rand::random::<u8>()).collect();
        Self { key }
    }

    /// Encrypt data (simple XOR for demo - use proper encryption in production)
    pub fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        if self.key.is_empty() {
            return Err(EncryptionError::InvalidKey("Key is empty".to_string()));
        }

        let mut encrypted = Vec::with_capacity(data.len());
        for (i, &byte) in data.iter().enumerate() {
            let key_byte = self.key[i % self.key.len()];
            encrypted.push(byte ^ key_byte);
        }

        Ok(encrypted)
    }

    /// Decrypt data (simple XOR for demo - use proper decryption in production)
    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        // XOR is symmetric, so decrypt is the same as encrypt
        self.encrypt(data)
    }

    /// Get key fingerprint for identification
    pub fn key_fingerprint(&self) -> String {
        let hash = blake3::hash(&self.key);
        hex::encode(&hash.as_bytes()[..8]) // First 8 bytes as hex
    }
}

/// Encryption trait for custom implementations
pub trait Encryptor: Send + Sync {
    /// Encrypt data
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, EncryptionError>;
    
    /// Decrypt data
    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, EncryptionError>;
    
    /// Get encryption algorithm name
    fn algorithm_name(&self) -> &'static str;
    
    /// Get key fingerprint
    fn key_fingerprint(&self) -> String;
}

impl Encryptor for EncryptionEngine {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        self.encrypt(data)
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        self.decrypt(data)
    }

    fn algorithm_name(&self) -> &'static str {
        "xor_demo" // In production: "aes-256-gcm" or similar
    }

    fn key_fingerprint(&self) -> String {
        self.key_fingerprint()
    }
}

/// Encryption-related errors
#[derive(Error, Debug)]
pub enum EncryptionError {
    /// Invalid encryption key
    #[error("Invalid encryption key: {0}")]
    InvalidKey(String),

    /// Encryption operation failed
    #[error("Encryption failed: {0}")]
    EncryptionFailed(String),

    /// Decryption operation failed
    #[error("Decryption failed: {0}")]
    DecryptionFailed(String),

    /// Key derivation failed
    #[error("Key derivation failed: {0}")]
    KeyDerivationFailed(String),

    /// Generic encryption error
    #[error("Encryption error: {0}")]
    Generic(String),
}

/// Key derivation utilities
pub struct KeyDerivation;

impl KeyDerivation {
    /// Derive key from password using PBKDF2 (simplified version)
    pub fn derive_key_from_password(
        password: &str,
        salt: &[u8],
        iterations: u32,
    ) -> Result<Vec<u8>, EncryptionError> {
        if password.is_empty() {
            return Err(EncryptionError::InvalidKey("Password is empty".to_string()));
        }

        // Simple key derivation (use proper PBKDF2 in production)
        let mut key = Vec::new();
        let password_bytes = password.as_bytes();
        
        for i in 0..32 {
            let mut hasher = blake3::Hasher::new();
            hasher.update(password_bytes);
            hasher.update(salt);
            hasher.update(&iterations.to_le_bytes());
            hasher.update(&(i as u32).to_le_bytes());
            
            let hash = hasher.finalize();
            key.push(hash.as_bytes()[0]);
        }

        Ok(key)
    }

    /// Generate random salt
    pub fn generate_salt() -> Vec<u8> {
        (0..16).map(|_| rand::random::<u8>()).collect()
    }
}

/// Authenticated encryption wrapper
pub struct AuthenticatedEncryption {
    encryptor: Box<dyn Encryptor>,
}

impl AuthenticatedEncryption {
    /// Create new authenticated encryption wrapper
    pub fn new(encryptor: Box<dyn Encryptor>) -> Self {
        Self { encryptor }
    }

    /// Encrypt with authentication
    pub fn encrypt_authenticated(&self, data: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        // Calculate MAC (Message Authentication Code)
        let mac = self.calculate_mac(data);
        
        // Encrypt data
        let encrypted_data = self.encryptor.encrypt(data)?;
        
        // Prepend MAC to encrypted data
        let mut result = Vec::with_capacity(mac.len() + encrypted_data.len());
        result.extend_from_slice(&mac);
        result.extend_from_slice(&encrypted_data);
        
        Ok(result)
    }

    /// Decrypt with authentication verification
    pub fn decrypt_authenticated(&self, data: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        if data.len() < 32 {
            return Err(EncryptionError::DecryptionFailed(
                "Data too short for authenticated encryption".to_string()
            ));
        }

        // Extract MAC and encrypted data
        let (mac, encrypted_data) = data.split_at(32);
        
        // Decrypt data
        let decrypted_data = self.encryptor.decrypt(encrypted_data)?;
        
        // Verify MAC
        let calculated_mac = self.calculate_mac(&decrypted_data);
        if mac != calculated_mac {
            return Err(EncryptionError::DecryptionFailed(
                "MAC verification failed".to_string()
            ));
        }
        
        Ok(decrypted_data)
    }

    /// Calculate MAC for data
    fn calculate_mac(&self, data: &[u8]) -> Vec<u8> {
        let hash = blake3::hash(data);
        hash.as_bytes()[..32].to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_roundtrip() {
        let engine = EncryptionEngine::new_random();
        let test_data = b"Hello, world! This is secret data.";

        let encrypted = engine.encrypt(test_data).unwrap();
        let decrypted = engine.decrypt(&encrypted).unwrap();

        assert_eq!(test_data.to_vec(), decrypted);
        assert_ne!(test_data.to_vec(), encrypted); // Should be different when encrypted
    }

    #[test]
    fn test_key_derivation() {
        let password = "test_password";
        let salt = KeyDerivation::generate_salt();
        
        let key1 = KeyDerivation::derive_key_from_password(password, &salt, 1000).unwrap();
        let key2 = KeyDerivation::derive_key_from_password(password, &salt, 1000).unwrap();
        
        assert_eq!(key1, key2); // Same password and salt should produce same key
        assert_eq!(key1.len(), 32); // Should be 32 bytes
    }

    #[test]
    fn test_authenticated_encryption() {
        let engine = EncryptionEngine::new_random();
        let auth_enc = AuthenticatedEncryption::new(Box::new(engine));
        let test_data = b"Secret message with authentication";

        let encrypted = auth_enc.encrypt_authenticated(test_data).unwrap();
        let decrypted = auth_enc.decrypt_authenticated(&encrypted).unwrap();

        assert_eq!(test_data.to_vec(), decrypted);
    }

    #[test]
    fn test_mac_verification_failure() {
        let engine = EncryptionEngine::new_random();
        let auth_enc = AuthenticatedEncryption::new(Box::new(engine));
        let test_data = b"Secret message";

        let mut encrypted = auth_enc.encrypt_authenticated(test_data).unwrap();
        
        // Corrupt the MAC
        encrypted[0] ^= 1;
        
        let result = auth_enc.decrypt_authenticated(&encrypted);
        assert!(result.is_err());
    }
}

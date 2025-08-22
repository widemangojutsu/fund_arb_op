#!/usr/bin/env python3
"""
Environment File Manager for persisting generated API keys
"""

import os
import re
from typing import Dict, Optional
from pathlib import Path


class EnvManager:
    """Manages .env file updates for persistent configuration"""
    
    def __init__(self, env_path: str = ".env"):
        self.env_path = Path(env_path)
        self.backup_path = Path(f"{env_path}.backup")
        
    def read_env(self) -> Dict[str, str]:
        """Read current .env file into dictionary"""
        env_vars = {}
        
        if not self.env_path.exists():
            return env_vars
            
        with open(self.env_path, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if line and not line.startswith('#'):
                    # Handle KEY=VALUE format
                    match = re.match(r'^([^=]+)=(.*)$', line)
                    if match:
                        key, value = match.groups()
                        env_vars[key.strip()] = value.strip()
        
        return env_vars
    
    def update_env_value(self, key: str, value: str, create_backup: bool = True) -> bool:
        """
        Update a specific value in the .env file while preserving structure
        
        Args:
            key: Environment variable name
            value: New value to set
            create_backup: Whether to create a backup before updating
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create backup if requested
            if create_backup and self.env_path.exists():
                import shutil
                shutil.copy2(self.env_path, self.backup_path)
            
            # Read the entire file
            lines = []
            key_found = False
            
            if self.env_path.exists():
                with open(self.env_path, 'r') as f:
                    for line in f:
                        # Check if this line contains our key
                        if line.strip() and not line.strip().startswith('#'):
                            match = re.match(rf'^{re.escape(key)}=.*$', line.strip())
                            if match:
                                # Replace the line with new value
                                lines.append(f"{key}={value}\n")
                                key_found = True
                            else:
                                lines.append(line)
                        else:
                            lines.append(line)
            
            # If key wasn't found, add it at the end of the credentials section
            if not key_found:
                # Find where to insert (after ETH_PRIVATE_KEY if it exists)
                insert_index = None
                for i, line in enumerate(lines):
                    if 'ETH_PRIVATE_KEY=' in line:
                        insert_index = i + 1
                        break
                
                if insert_index is not None:
                    lines.insert(insert_index, f"{key}={value}\n")
                else:
                    # Just append at the end
                    lines.append(f"{key}={value}\n")
            
            # Write back to file
            with open(self.env_path, 'w') as f:
                f.writelines(lines)
            
            # Also update the environment variable for current process
            os.environ[key] = value
            
            return True
            
        except Exception as e:
            print(f"Error updating .env file: {e}")
            return False
    
    def get_env_value(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get a value from .env file"""
        env_vars = self.read_env()
        return env_vars.get(key, default)
    
    def restore_backup(self) -> bool:
        """Restore .env file from backup"""
        try:
            if self.backup_path.exists():
                import shutil
                shutil.copy2(self.backup_path, self.env_path)
                return True
            return False
        except Exception as e:
            print(f"Error restoring backup: {e}")
            return False


def save_lighter_api_key(api_key: str) -> bool:
    """
    Save the generated Lighter API key to .env file
    
    Args:
        api_key: The generated API private key
        
    Returns:
        True if successful
    """
    manager = EnvManager()
    success = manager.update_env_value("LIGHTER_API_PRIVATE_KEY", api_key)
    
    if success:
        print(f"✅ Lighter API key saved to .env file")
        print(f"   Key (first 16 chars): {api_key[:16]}...")
    else:
        print(f"⚠️ Failed to save Lighter API key to .env file")
        print(f"   You should manually add to .env:")
        print(f"   LIGHTER_API_PRIVATE_KEY={api_key}")
    
    return success


def check_lighter_api_key() -> Optional[str]:
    """
    Check if Lighter API key exists in .env file
    
    Returns:
        The API key if found, None otherwise
    """
    manager = EnvManager()
    api_key = manager.get_env_value("LIGHTER_API_PRIVATE_KEY")
    
    if api_key and api_key.strip():
        return api_key.strip()
    return None
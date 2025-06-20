"""
Configuration management for IMDB Ratings application.

This module centralizes all configuration settings using Pydantic for 
validation and type safety. Configuration can be loaded from environment 
variables or a .env file.
"""

from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


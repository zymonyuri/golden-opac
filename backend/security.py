import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from passlib.context import CryptContext
from jose import jwt, JWTError

load_dotenv()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-this")
JWT_ALG = "HS256"
JWT_EXPIRE_DAYS = int(os.getenv("JWT_EXPIRE_DAYS", "7"))


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)


def create_access_token(subject: str) -> str:
    expire = datetime.utcnow() + timedelta(days=JWT_EXPIRE_DAYS)
    payload = {"sub": subject, "exp": expire}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
    except JWTError:
        raise ValueError("Invalid or expired token")
import asyncio
import json
import logging
from typing import Dict, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import threading
import time

from producer import KafkaProducer
from consumer import KafkaConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("events-service")

app = FastAPI(
    title="CinemaAbyss Events Service",
    description="Микросервис для обработки событий с использованием Kafka",
    version="1.0.0"
)

# Initialize Kafka components
producer = KafkaProducer()
consumer = KafkaConsumer()


# Event models
class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: int = None
    rating: float = None
    genres: list = None
    description: str = None


class UserEvent(BaseModel):
    user_id: int
    username: str = None
    email: str = None
    action: str
    timestamp: str


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: str
    method_type: str = None


class EventResponse(BaseModel):
    status: str
    partition: int
    offset: int
    event: Dict[str, Any]


@app.on_event("startup")
async def startup_event():
    """Start Kafka consumer on application startup"""
    logger.info("Starting Events Service...")
    # Start consumer in background thread
    consumer_thread = threading.Thread(target=consumer.start_consuming, daemon=True)
    consumer_thread.start()
    logger.info("Kafka consumer started in background thread")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": True}


@app.get("/api/events/health")
async def events_health():
    """Events service health check"""
    return {"status": True}


@app.post("/api/events/movie", response_model=EventResponse, status_code=201)  # Добавлен status_code=201
async def create_movie_event(event: MovieEvent):
    try:
        kafka_event = {
            "id": f"movie-{event.movie_id}-{event.action}",
            "type": "movie",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "payload": event.dict()
        }

        result = producer.send_message("movie-events", kafka_event)
        logger.info(f"Movie event sent to Kafka: {kafka_event}")

        return EventResponse(
            status="success",
            partition=result["partition"],
            offset=result["offset"],
            event=kafka_event
        )
    except Exception as e:
        logger.error(f"Error sending movie event: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/events/user", response_model=EventResponse, status_code=201)  # Добавлен status_code=201
async def create_user_event(event: UserEvent):
    try:
        kafka_event = {
            "id": f"user-{event.user_id}-{event.action}",
            "type": "user",
            "timestamp": event.timestamp,
            "payload": event.dict()
        }

        result = producer.send_message("user-events", kafka_event)
        logger.info(f"User event sent to Kafka: {kafka_event}")

        return EventResponse(
            status="success",
            partition=result["partition"],
            offset=result["offset"],
            event=kafka_event
        )
    except Exception as e:
        logger.error(f"Error sending user event: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/events/payment", response_model=EventResponse, status_code=201)  # Добавлен status_code=201
async def create_payment_event(event: PaymentEvent):
    try:
        kafka_event = {
            "id": f"payment-{event.payment_id}-{event.status}",
            "type": "payment",
            "timestamp": event.timestamp,
            "payload": event.dict()
        }

        result = producer.send_message("payment-events", kafka_event)
        logger.info(f"Payment event sent to Kafka: {kafka_event}")

        return EventResponse(
            status="success",
            partition=result["partition"],
            offset=result["offset"],
            event=kafka_event
        )
    except Exception as e:
        logger.error(f"Error sending payment event: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8082,
        reload=True,
        log_level="info"
    )
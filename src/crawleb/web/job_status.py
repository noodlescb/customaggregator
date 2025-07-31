import asyncio
import logging
from typing import Dict, Optional, Any
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    IDLE = "idle"
    RUNNING = "running" 
    COMPLETED = "completed"
    ERROR = "error"


class JobStatusTracker:
    """Global job status tracker for managing background tasks."""
    
    def __init__(self):
        self._status: Dict[str, Dict[str, Any]] = {
            "refresh_all": {
                "status": JobStatus.IDLE,
                "current_step": "",
                "total_steps": 3,
                "current_step_number": 0,
                "started_at": None,
                "completed_at": None,
                "error": None,
                "results": {}
            },
            "crawl": {
                "status": JobStatus.IDLE,
                "started_at": None,
                "completed_at": None,
                "error": None,
                "results": {}
            },
            "research": {
                "status": JobStatus.IDLE,
                "started_at": None,
                "completed_at": None,
                "error": None,
                "results": {}
            },
            "trending": {
                "status": JobStatus.IDLE,
                "started_at": None,
                "completed_at": None,
                "error": None,
                "results": {}
            }
        }
    
    def get_status(self, job_name: str) -> Dict[str, Any]:
        """Get status of a specific job."""
        if job_name not in self._status:
            return {"status": JobStatus.IDLE, "error": f"Unknown job: {job_name}"}
        return self._status[job_name].copy()
    
    def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all jobs."""
        return {k: v.copy() for k, v in self._status.items()}
    
    def start_job(self, job_name: str, current_step: str = ""):
        """Mark a job as started."""
        if job_name not in self._status:
            # Create a basic job entry for unknown jobs
            self._status[job_name] = {
                "status": JobStatus.IDLE,
                "current_step": "",
                "started_at": None,
                "completed_at": None,
                "error": None,
                "results": {}
            }
        
        self._status[job_name].update({
            "status": JobStatus.RUNNING,
            "started_at": datetime.now(),
            "completed_at": None,
            "error": None,
            "current_step": current_step
        })
        
        if job_name == "refresh_all":
            self._status[job_name]["current_step_number"] = 1
        
        logger.info(f"Started job: {job_name}")
    
    def update_job_step(self, job_name: str, step: str, step_number: int = None):
        """Update the current step of a running job."""
        if job_name not in self._status:
            return
        
        self._status[job_name]["current_step"] = step
        if step_number is not None:
            self._status[job_name]["current_step_number"] = step_number
        
        logger.info(f"Job {job_name} - Step {step_number}: {step}")
    
    def complete_job(self, job_name: str, results: Dict[str, Any] = None):
        """Mark a job as completed."""
        if job_name not in self._status:
            return
        
        self._status[job_name].update({
            "status": JobStatus.COMPLETED,
            "completed_at": datetime.now(),
            "current_step": "Completed",
            "results": results or {}
        })
        
        logger.info(f"Completed job: {job_name}")
    
    def fail_job(self, job_name: str, error: str):
        """Mark a job as failed."""
        if job_name not in self._status:
            return
        
        self._status[job_name].update({
            "status": JobStatus.ERROR,
            "completed_at": datetime.now(),
            "error": error,
            "current_step": f"Failed: {error}"
        })
        
        logger.error(f"Job {job_name} failed: {error}")
    
    def is_job_running(self, job_name: str) -> bool:
        """Check if a specific job is currently running."""
        return self._status.get(job_name, {}).get("status") == JobStatus.RUNNING
    
    def is_any_job_running(self) -> bool:
        """Check if any job is currently running."""
        return any(job["status"] == JobStatus.RUNNING for job in self._status.values())
    
    def reset_job(self, job_name: str):
        """Reset a job to idle state."""
        if job_name not in self._status:
            return
        
        self._status[job_name].update({
            "status": JobStatus.IDLE,
            "current_step": "",
            "started_at": None,
            "completed_at": None,
            "error": None,
            "results": {}
        })
        
        if job_name == "refresh_all":
            self._status[job_name]["current_step_number"] = 0


# Global instance
job_tracker = JobStatusTracker()
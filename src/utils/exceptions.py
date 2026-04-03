"""Exception handlers for the application"""

# Add the missing AppException class
class AppException(Exception):
    """Base application exception"""
    pass

# Add other exception classes as needed
class NotFoundException(AppException):
    """Resource not found exception"""
    pass

class ValidationException(AppException):
    """Validation error exception"""
    pass

# Exception handlers (if using FastAPI)
try:
    from fastapi import Request, status
    from fastapi.responses import JSONResponse
    
    def not_found_handler(request: Request, exc: Exception):
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"message": "Resource not found", "detail": str(exc)}
        )
    
    def validation_exception_handler(request: Request, exc: Exception):
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"message": "Validation error", "detail": str(exc)}
        )
    
    def generic_exception_handler(request: Request, exc: Exception):
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": "Internal server error", "detail": str(exc)}
        )
    
    def app_exception_handler(request: Request, exc: AppException):
        """Handle application exceptions"""
        return JSONResponse(
            status_code=getattr(exc, 'status_code', 400),
            content={"message": str(exc), "type": exc.__class__.__name__}
        )
    
except ImportError:
    # Fallback if FastAPI is not installed
    def not_found_handler(request, exc):
        return {"message": "Resource not found", "detail": str(exc)}, 404
    
    def validation_exception_handler(request, exc):
        return {"message": "Validation error", "detail": str(exc)}, 422
    
    def generic_exception_handler(request, exc):
        return {"message": "Internal server error", "detail": str(exc)}, 500
    
    def app_exception_handler(request, exc):
        """Handle application exceptions"""
        if isinstance(exc, AppException):
            return {"message": str(exc), "type": exc.__class__.__name__}, 400
        return generic_exception_handler(request, exc)
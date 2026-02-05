import { useState, useRef, useEffect } from 'react';
import { Skeleton } from 'antd';

interface LazyImageProps {
  src: string;
  alt: string;
  width?: number | string;
  height?: number | string;
  style?: React.CSSProperties;
  className?: string;
  placeholder?: React.ReactNode;
}

/**
 * Lazy load image component with intersection observer
 */
export function LazyImage({
  src,
  alt,
  width,
  height,
  style,
  className,
  placeholder,
}: LazyImageProps) {
  const [isLoaded, setIsLoaded] = useState(false);
  const [isInView, setIsInView] = useState(false);
  const imgRef = useRef<HTMLImageElement>(null);

  useEffect(() => {
    // Skip if IntersectionObserver is not available
    if (typeof window === 'undefined' || !window.IntersectionObserver) {
      setIsInView(true);
      return;
    }

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setIsInView(true);
          observer.disconnect();
        }
      },
      {
        rootMargin: '50px', // Start loading 50px before element comes into view
      }
    );

    if (imgRef.current) {
      observer.observe(imgRef.current);
    }

    return () => {
      observer.disconnect();
    };
  }, []);

  const handleLoad = () => {
    setIsLoaded(true);
  };

  const defaultStyle: React.CSSProperties = {
    ...style,
    width,
    height,
    opacity: isLoaded ? 1 : 0,
    transition: 'opacity 0.3s ease-in-out',
    display: isLoaded ? 'block' : 'none',
  };

  const placeholderStyle: React.CSSProperties = {
    width,
    height,
    display: !isLoaded ? 'block' : 'none',
  };

  return (
    <div style={{ position: 'relative' }} className={className}>
      {/* Actual image */}
      {isInView && (
        <img
          ref={imgRef}
          src={src}
          alt={alt}
          style={defaultStyle}
          onLoad={handleLoad}
        />
      )}

      {/* Placeholder */}
      {(!isInView || !isLoaded) && (
        <div style={placeholderStyle}>
          {placeholder || <Skeleton.Image active style={{ width: '100%', height: '100%' }} />}
        </div>
      )}
    </div>
  );
}

export default LazyImage;

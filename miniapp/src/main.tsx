import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './styles/index.css'

function preventZoomGestures() {
  if (typeof document === "undefined") {
    return;
  }

  const preventDefault = (event: Event) => {
    event.preventDefault();
  };

  document.addEventListener("gesturestart", preventDefault, { passive: false });
  document.addEventListener("gesturechange", preventDefault, { passive: false });
  document.addEventListener("gestureend", preventDefault, { passive: false });
  document.addEventListener("dblclick", preventDefault, { passive: false });

  let lastTouchEnd = 0;
  document.addEventListener(
    "touchend",
    (event: TouchEvent) => {
      const now = Date.now();
      if (now - lastTouchEnd <= 300) {
        event.preventDefault();
      }
      lastTouchEnd = now;
    },
    { passive: false },
  );
}

preventZoomGestures();

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)

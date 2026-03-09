use std::sync::Arc;
use std::time::Duration;

use bits::{Bits, Job, JobResult, PollOutcome};
use futures::TryStreamExt;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyModule, PyType};

#[pyclass(name = "Bits")]
struct BitsPy {
    inner: Arc<Bits>,
}

#[pymethods]
impl BitsPy {
    #[classmethod]
    fn from_config<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        config: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let bits = Bits::from_config(&config).map_err(to_value_error)?;
            Ok(BitsPy {
                inner: Arc::new(bits),
            })
        })
    }

    fn submit<'py>(
        &self,
        py: Python<'py>,
        request: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request_value: serde_json::Value =
            pythonize::depythonize(&request).map_err(to_value_error)?;
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = inner.submit(Job::new(request_value));
            Ok(handle.id)
        })
    }

    #[pyo3(signature = (job_id, timeout_secs=None))]
    fn poll<'py>(
        &self,
        py: Python<'py>,
        job_id: String,
        timeout_secs: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let timeout = match timeout_secs {
            Some(secs) if !secs.is_finite() || secs < 0.0 => {
                return Err(PyValueError::new_err(
                    "timeout_secs must be a finite non-negative number",
                ));
            }
            Some(secs) => Some(Duration::from_secs_f64(secs)),
            None => None,
        };
        let inner = Arc::clone(&self.inner);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let outcome = inner.poll(&job_id, timeout).await;
            poll_outcome_to_py(outcome).await
        })
    }

    fn cancel<'py>(&self, py: Python<'py>, job_id: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.cancel(&job_id);
            Ok(())
        })
    }
}

async fn poll_outcome_to_py(outcome: PollOutcome) -> PyResult<Py<PyAny>> {
    match outcome {
        PollOutcome::Ready(result) => {
            let result_obj = job_result_to_py(result).await?;
            Python::attach(|py| {
                let payload = PyDict::new(py);
                payload.set_item("status", "ready")?;
                payload.set_item("result", result_obj.bind(py))?;
                Ok(payload.into_any().unbind())
            })
        }
        PollOutcome::Pending { id } => Python::attach(|py| {
            let payload = PyDict::new(py);
            payload.set_item("status", "pending")?;
            payload.set_item("id", id)?;
            Ok(payload.into_any().unbind())
        }),
        PollOutcome::NotFound => Python::attach(|py| {
            let payload = PyDict::new(py);
            payload.set_item("status", "not_found")?;
            Ok(payload.into_any().unbind())
        }),
        PollOutcome::JobLost => Python::attach(|py| {
            let payload = PyDict::new(py);
            payload.set_item("status", "job_lost")?;
            Ok(payload.into_any().unbind())
        }),
    }
}

async fn job_result_to_py(result: JobResult) -> PyResult<Py<PyAny>> {
    match result {
        JobResult::Success {
            content_type,
            size,
            stream,
        } => {
            let body = read_stream(stream).await?;
            Python::attach(|py| {
                let payload = PyDict::new(py);
                payload.set_item("status", "success")?;
                payload.set_item("content_type", content_type)?;
                payload.set_item("size", size)?;
                payload.set_item("body", PyBytes::new(py, &body))?;
                Ok(payload.into_any().unbind())
            })
        }
        JobResult::Redirect { location, message } => Python::attach(|py| {
            let payload = PyDict::new(py);
            payload.set_item("status", "redirect")?;
            payload.set_item("location", location)?;
            payload.set_item("message", message)?;
            Ok(payload.into_any().unbind())
        }),
        JobResult::Error { message } => Python::attach(|py| {
            let payload = PyDict::new(py);
            payload.set_item("status", "error")?;
            payload.set_item("message", message)?;
            Ok(payload.into_any().unbind())
        }),
        JobResult::Failed { reason } => Python::attach(|py| {
            let payload = PyDict::new(py);
            payload.set_item("status", "failed")?;
            payload.set_item("reason", reason)?;
            Ok(payload.into_any().unbind())
        }),
        JobResult::Cancelled => Python::attach(|py| {
            let payload = PyDict::new(py);
            payload.set_item("status", "cancelled")?;
            Ok(payload.into_any().unbind())
        }),
        JobResult::ClientGone => Python::attach(|py| {
            let payload = PyDict::new(py);
            payload.set_item("status", "client_gone")?;
            Ok(payload.into_any().unbind())
        }),
    }
}

async fn read_stream(
    mut stream: Box<
        dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Unpin,
    >,
) -> PyResult<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = stream.try_next().await.map_err(to_runtime_error)? {
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn to_value_error<E: std::fmt::Display>(err: E) -> PyErr {
    PyValueError::new_err(err.to_string())
}

fn to_runtime_error<E: std::fmt::Display>(err: E) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

#[pymodule]
fn bits_py(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BitsPy>()?;
    Ok(())
}

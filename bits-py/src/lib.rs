// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use bits::{
    Job, JobResult, PollOutcome, RuntimeActionFactory,
    actions::{
        Action, ActionError, CheckAction, CheckResult, TargetAction, TargetResult, TransformAction,
        TransformResult,
    },
    register_runtime_action,
};
use bytes::Bytes;
use futures::TryStreamExt;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyModule, PyType};

// ============================================================
//  Outcome types exposed to Python
// ============================================================

/// Returned by :meth:`CheckAction.evaluate` to indicate the job may proceed.
#[pyclass(name = "Pass", skip_from_py_object)]
pub struct PyPass;

#[pymethods]
impl PyPass {
    #[new]
    fn new() -> Self {
        PyPass
    }
    fn __repr__(&self) -> &str {
        "Pass()"
    }
}

/// Returned by :meth:`CheckAction.evaluate` or :meth:`TransformAction.execute` to
/// reject the current pipeline branch (the router will try the next route).
#[pyclass(name = "Reject", from_py_object)]
#[derive(Clone)]
pub struct PyReject {
    pub reason: String,
    pub silent: bool,
}

#[pymethods]
impl PyReject {
    #[new]
    #[pyo3(signature = (reason, silent = true))]
    fn new(reason: String, silent: bool) -> Self {
        PyReject { reason, silent }
    }
    fn __repr__(&self) -> String {
        format!("Reject({:?})", self.reason)
    }
}

/// Returned by :meth:`TransformAction.execute` to indicate the pipeline should continue.
#[pyclass(name = "Continue", skip_from_py_object)]
pub struct PyContinue;

#[pymethods]
impl PyContinue {
    #[new]
    fn new() -> Self {
        PyContinue
    }
    fn __repr__(&self) -> &str {
        "Continue()"
    }
}

/// Returned by :meth:`TargetAction.dispatch` on a successful response.
///
/// ``body`` is ``bytes`` or ``str`` (auto-encoded as UTF-8).
/// ``content_type`` defaults to ``"application/octet-stream"`` for bytes or
/// ``"text/plain; charset=utf-8"`` for str.
///
/// Use :meth:`Success.json` to send a Python dict as JSON.
#[pyclass(name = "Success", from_py_object)]
#[derive(Clone)]
pub struct PySuccess {
    pub body: Vec<u8>,
    pub content_type: String,
}

#[pymethods]
impl PySuccess {
    #[new]
    #[pyo3(signature = (body, content_type = None))]
    fn new(body: &Bound<'_, PyAny>, content_type: Option<String>) -> PyResult<Self> {
        let (raw, default_ct) = if let Ok(b) = body.cast::<PyBytes>() {
            (
                b.as_bytes().to_vec(),
                "application/octet-stream".to_string(),
            )
        } else if let Ok(s) = body.extract::<String>() {
            (s.into_bytes(), "text/plain; charset=utf-8".to_string())
        } else {
            return Err(PyTypeError::new_err("Success body must be bytes or str"));
        };
        Ok(PySuccess {
            body: raw,
            content_type: content_type.unwrap_or(default_ct),
        })
    }

    /// Serialise a Python value (dict, list, …) to JSON using ``json.dumps``.
    #[classmethod]
    fn json(_cls: &Bound<'_, PyType>, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let json_bytes = Python::attach(|py| -> PyResult<Vec<u8>> {
            let json_mod = py.import("json")?;
            let s: String = json_mod.call_method1("dumps", (value,))?.extract()?;
            Ok(s.into_bytes())
        })?;
        Ok(PySuccess {
            body: json_bytes,
            content_type: "application/json".to_string(),
        })
    }

    fn __repr__(&self) -> String {
        format!("Success({} bytes, {})", self.body.len(), self.content_type)
    }
}

/// Returned by :meth:`TargetAction.dispatch` to redirect the client.
#[pyclass(name = "Redirect", from_py_object)]
#[derive(Clone)]
pub struct PyRedirect {
    pub location: String,
    pub message: String,
}

#[pymethods]
impl PyRedirect {
    #[new]
    #[pyo3(signature = (location, message = String::new()))]
    fn new(location: String, message: String) -> Self {
        PyRedirect { location, message }
    }
    fn __repr__(&self) -> String {
        format!("Redirect({:?})", self.location)
    }
}

/// Returned by :meth:`TargetAction.dispatch` to signal a job-level error
/// (invalid request, authorisation failure, etc.).
#[pyclass(name = "Error", from_py_object)]
#[derive(Clone)]
pub struct PyError {
    pub message: String,
}

#[pymethods]
impl PyError {
    #[new]
    fn new(message: String) -> Self {
        PyError { message }
    }
    fn __repr__(&self) -> String {
        format!("Error({:?})", self.message)
    }
}

// ============================================================
//  ABC base classes exposed to Python
// ============================================================

/// Abstract base class for check actions.
///
/// Subclass and implement an async ``evaluate`` method::
///
///     class MyCheck(CheckAction):
///         def __init__(self, **kwargs):
///             ...
///         async def evaluate(self, job) -> Pass | Reject:
///             ...
///
/// Register with :func:`register_action`.
#[pyclass(name = "CheckAction", subclass)]
pub struct PyCheckAction;

#[pymethods]
impl PyCheckAction {
    /// Accept any keyword arguments so subclass ``__init__`` methods with
    /// custom parameters are not blocked by pyo3's ``__new__`` forwarding.
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: &Bound<'_, pyo3::types::PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        PyCheckAction
    }
}

/// Abstract base class for transform actions.
///
/// Subclass and implement an async ``execute`` method::
///
///     class MyTransform(TransformAction):
///         def __init__(self, **kwargs):
///             ...
///         async def execute(self, job) -> Continue | Reject:
///             ...
///
/// Mutations to ``job.request`` and ``job.metadata`` are propagated back into
/// the pipeline automatically.
///
/// Register with :func:`register_action`.
#[pyclass(name = "TransformAction", subclass)]
pub struct PyTransformAction;

#[pymethods]
impl PyTransformAction {
    /// Accept any keyword arguments so subclass ``__init__`` methods with
    /// custom parameters are not blocked by pyo3's ``__new__`` forwarding.
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: &Bound<'_, pyo3::types::PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        PyTransformAction
    }
}

/// Abstract base class for target actions.
///
/// Subclass and implement an async ``dispatch`` method::
///
///     class MyTarget(TargetAction):
///         def __init__(self, **kwargs):
///             ...
///         async def dispatch(self, job) -> Success | Redirect | Error | Reject:
///             ...
///
/// Register with :func:`register_action`.
#[pyclass(name = "TargetAction", subclass)]
pub struct PyTargetAction;

#[pymethods]
impl PyTargetAction {
    /// Accept any keyword arguments so subclass ``__init__`` methods with
    /// custom parameters are not blocked by pyo3's ``__new__`` forwarding.
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: &Bound<'_, pyo3::types::PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        PyTargetAction
    }
}

// ============================================================
//  Job view passed to Python action methods
// ============================================================

/// Read/write view of a job passed to action callbacks.
///
/// Attributes:
///
/// - ``id`` (str, read-only) — unique job identifier.
/// - ``request`` (dict) — working request; transform actions may mutate this.
/// - ``metadata`` (dict) — pipeline metadata; transform actions may mutate this.
/// - ``user`` (dict, read-only) — user context attached at submission.
#[pyclass(name = "Job")]
pub struct PyJob {
    pub id: String,
    pub request: Py<PyAny>,
    pub metadata: Py<PyAny>,
    pub user: Py<PyAny>,
}

#[pymethods]
impl PyJob {
    #[getter]
    fn id(&self) -> &str {
        &self.id
    }
    #[getter]
    fn request<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.request.bind(py).clone()
    }
    #[setter]
    fn set_request(&mut self, value: Py<PyAny>) {
        self.request = value;
    }
    #[getter]
    fn metadata<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.metadata.bind(py).clone()
    }
    #[setter]
    fn set_metadata(&mut self, value: Py<PyAny>) {
        self.metadata = value;
    }
    #[getter]
    fn user<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.user.bind(py).clone()
    }
    fn __repr__(&self) -> String {
        format!("Job(id={:?})", self.id)
    }
}

fn job_to_py(py: Python<'_>, job: &Job) -> PyResult<Py<PyJob>> {
    let request: Py<PyAny> = pythonize::pythonize(py, &job.request)?.unbind().into_any();
    let metadata: Py<PyAny> = pythonize::pythonize(py, &job.metadata)?.unbind().into_any();
    let user: Py<PyAny> = pythonize::pythonize(py, &job.user)?.unbind().into_any();
    Py::new(
        py,
        PyJob {
            id: job.id.clone(),
            request,
            metadata,
            user,
        },
    )
}

// ============================================================
//  Helpers: validate action class at registration time
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq)]
enum ActionKind {
    Check,
    Transform,
    Target,
}

fn detect_kind(_py: Python<'_>, cls: &Bound<'_, PyAny>) -> PyResult<ActionKind> {
    let cls_type = cls
        .cast::<PyType>()
        .map_err(|_| PyTypeError::new_err("register_action: second argument must be a class"))?;
    let is_check = cls_type.is_subclass_of::<PyCheckAction>()?;
    let is_transform = cls_type.is_subclass_of::<PyTransformAction>()?;
    let is_target = cls_type.is_subclass_of::<PyTargetAction>()?;

    match (is_check, is_transform, is_target) {
        (true, false, false) => Ok(ActionKind::Check),
        (false, true, false) => Ok(ActionKind::Transform),
        (false, false, true) => Ok(ActionKind::Target),
        (false, false, false) => Err(PyTypeError::new_err(
            "action class must subclass CheckAction, TransformAction, or TargetAction",
        )),
        _ => Err(PyTypeError::new_err(
            "action class must subclass exactly one of CheckAction, TransformAction, TargetAction",
        )),
    }
}

/// Verify that `method_name` exists on `cls` and is a coroutine function.
fn require_async_method(py: Python<'_>, cls: &Bound<'_, PyAny>, method_name: &str) -> PyResult<()> {
    let method = cls.getattr(method_name).map_err(|_| {
        PyTypeError::new_err(format!(
            "action class must define an async method '{}'",
            method_name
        ))
    })?;
    let asyncio = py.import("asyncio")?;
    let is_coro: bool = asyncio
        .call_method1("iscoroutinefunction", (&method,))?
        .extract()?;
    if !is_coro {
        return Err(PyTypeError::new_err(format!(
            "action method '{}' must be an async (coroutine) function",
            method_name
        )));
    }
    Ok(())
}

// ============================================================
//  Helper: run a Python coroutine to completion using asyncio.run()
//
//  Because bits' Rust async tasks run on a Tokio runtime that has no
//  Python event loop attached, we cannot use `pyo3_async_runtimes::tokio::
//  into_future` directly (it requires a running asyncio loop in the current
//  thread).  Instead we call `asyncio.run(coro)` via a blocking thread so
//  the coroutine gets its own temporary event loop.
// ============================================================

fn run_python_coro(coro: Py<PyAny>) -> Result<Py<PyAny>, ActionError> {
    Python::attach(|py| -> Result<Py<PyAny>, ActionError> {
        let asyncio = py
            .import("asyncio")
            .map_err(|e| ActionError::ConfigError(format!("import asyncio: {}", e)))?;
        let result = asyncio
            .call_method1("run", (coro.bind(py),))
            .map_err(|e| ActionError::ConfigError(format!("asyncio.run() raised: {}", e)))?;
        Ok(result.unbind())
    })
}

// ============================================================
//  Rust adapter: CheckAction wrapping a Python instance
// ============================================================

struct PyCheckAdapter {
    instance: Py<PyAny>,
}

unsafe impl Send for PyCheckAdapter {}
unsafe impl Sync for PyCheckAdapter {}

impl std::fmt::Debug for PyCheckAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PyCheckAdapter")
    }
}

#[async_trait::async_trait]
impl CheckAction for PyCheckAdapter {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        let coro: Py<PyAny> = Python::attach(|py| -> PyResult<Py<PyAny>> {
            let instance = self.instance.clone_ref(py);
            let py_job = job_to_py(py, job)?;
            instance.call_method1(py, "evaluate", (py_job,))
        })
        .map_err(|e| ActionError::ConfigError(format!("evaluate() call failed: {}", e)))?;

        let result_obj: Py<PyAny> = tokio::task::spawn_blocking(move || run_python_coro(coro))
            .await
            .map_err(|e| ActionError::ConfigError(format!("spawn_blocking failed: {}", e)))??;

        Python::attach(|py| {
            let obj = result_obj.bind(py);
            if obj.is_instance_of::<PyPass>() {
                Ok(CheckResult::Pass)
            } else if let Ok(r) = obj.extract::<PyRef<PyReject>>() {
                Ok(CheckResult::Reject {
                    reason: r.reason.clone(),
                    silent: r.silent,
                })
            } else {
                let type_name = obj
                    .get_type()
                    .name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| "unknown".to_string());
                Err(ActionError::ConfigError(format!(
                    "evaluate() must return Pass or Reject, got: {}",
                    type_name
                )))
            }
        })
    }
}

// ============================================================
//  Rust adapter: TransformAction wrapping a Python instance
// ============================================================

struct PyTransformAdapter {
    instance: Py<PyAny>,
}

unsafe impl Send for PyTransformAdapter {}
unsafe impl Sync for PyTransformAdapter {}

impl std::fmt::Debug for PyTransformAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PyTransformAdapter")
    }
}

#[async_trait::async_trait]
impl TransformAction for PyTransformAdapter {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        // Materialise the job into a Python PyJob so Python can read and mutate it.
        let py_job_obj: Py<PyJob> = Python::attach(|py| job_to_py(py, job))
            .map_err(|e| ActionError::ConfigError(format!("job serialisation failed: {}", e)))?;

        let (coro, py_job_for_read): (Py<PyAny>, Py<PyJob>) = Python::attach(|py| {
            let instance = self.instance.clone_ref(py);
            let coro = instance.call_method1(py, "execute", (py_job_obj.bind(py),))?;
            let job_ref = py_job_obj.clone_ref(py);
            Ok::<_, PyErr>((coro, job_ref))
        })
        .map_err(|e| ActionError::ConfigError(format!("execute() call failed: {}", e)))?;

        let result_obj: Py<PyAny> = tokio::task::spawn_blocking(move || run_python_coro(coro))
            .await
            .map_err(|e| ActionError::ConfigError(format!("spawn_blocking failed: {}", e)))??;

        // Read back mutations from the PyJob into the Rust Job.
        Python::attach(|py| -> Result<(), ActionError> {
            let borrow = py_job_for_read.borrow(py);
            job.request = pythonize::depythonize(borrow.request.bind(py))
                .map_err(|e| ActionError::ConfigError(format!("request deserialise: {}", e)))?;
            job.metadata = pythonize::depythonize(borrow.metadata.bind(py))
                .map_err(|e| ActionError::ConfigError(format!("metadata deserialise: {}", e)))?;
            Ok(())
        })?;

        Python::attach(|py| {
            let obj = result_obj.bind(py);
            if obj.is_instance_of::<PyContinue>() {
                Ok(TransformResult::Continue)
            } else if let Ok(r) = obj.extract::<PyRef<PyReject>>() {
                Ok(TransformResult::Reject {
                    reason: r.reason.clone(),
                    silent: r.silent,
                })
            } else {
                let type_name = obj
                    .get_type()
                    .name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| "unknown".to_string());
                Err(ActionError::ConfigError(format!(
                    "execute() must return Continue or Reject, got: {}",
                    type_name
                )))
            }
        })
    }
}

// ============================================================
//  Rust adapter: TargetAction wrapping a Python instance
// ============================================================

struct PyTargetAdapter {
    instance: Py<PyAny>,
}

unsafe impl Send for PyTargetAdapter {}
unsafe impl Sync for PyTargetAdapter {}

impl std::fmt::Debug for PyTargetAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PyTargetAdapter")
    }
}

#[async_trait::async_trait]
impl TargetAction for PyTargetAdapter {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        let py_job_obj: Py<PyJob> = Python::attach(|py| job_to_py(py, job))
            .map_err(|e| ActionError::ConfigError(format!("job serialisation failed: {}", e)))?;

        let coro: Py<PyAny> = Python::attach(|py| {
            let instance = self.instance.clone_ref(py);
            instance.call_method1(py, "dispatch", (py_job_obj.bind(py),))
        })
        .map_err(|e| ActionError::ConfigError(format!("dispatch() call failed: {}", e)))?;

        let result_obj: Py<PyAny> = tokio::task::spawn_blocking(move || run_python_coro(coro))
            .await
            .map_err(|e| ActionError::ConfigError(format!("spawn_blocking failed: {}", e)))??;

        Python::attach(|py| {
            let obj = result_obj.bind(py);
            if let Ok(s) = obj.extract::<PyRef<PySuccess>>() {
                let body = Bytes::from(s.body.clone());
                let ct = s.content_type.clone();
                let size = body.len() as i64;
                let stream = Box::new(futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(
                    body,
                )]))
                    as Box<
                        dyn futures::Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin,
                    >;
                Ok(TargetResult::Complete(JobResult::Success {
                    content_type: ct,
                    size,
                    stream,
                }))
            } else if let Ok(r) = obj.extract::<PyRef<PyRedirect>>() {
                Ok(TargetResult::Complete(JobResult::Redirect {
                    location: r.location.clone(),
                    message: r.message.clone(),
                    // Python dispatchers don't surface content metadata; the
                    // BOBS/S3 worker delivery path is where it originates.
                    content_type: None,
                    content_length: None,
                }))
            } else if let Ok(e) = obj.extract::<PyRef<PyError>>() {
                Ok(TargetResult::Complete(JobResult::Error {
                    message: e.message.clone(),
                }))
            } else if let Ok(r) = obj.extract::<PyRef<PyReject>>() {
                Ok(TargetResult::Reject {
                    reason: r.reason.clone(),
                    silent: r.silent,
                })
            } else {
                let type_name = obj
                    .get_type()
                    .name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| "unknown".to_string());
                Err(ActionError::ConfigError(format!(
                    "dispatch() must return Success, Redirect, Error, or Reject, got: {}",
                    type_name
                )))
            }
        })
    }
}

// ============================================================
//  register_action() Python function
// ============================================================

/// Register a Python action class with the bits runtime registry.
///
/// The class must:
///
/// - Subclass exactly one of :class:`CheckAction`, :class:`TransformAction`, or
///   :class:`TargetAction`.
/// - Implement the corresponding async method (``evaluate``, ``execute``, or
///   ``dispatch``).
/// - Accept keyword arguments in ``__init__`` whose names match the YAML config
///   keys (everything except ``type`` and ``dispatcher``).
///
/// Must be called **before** :meth:`Bits.from_config`.
///
/// Example::
///
///     from bits_py import CheckAction, Pass, Reject, register_action
///
///     class HasRole(CheckAction):
///         def __init__(self, role: str):
///             self.role = role
///
///         async def evaluate(self, job) -> Pass | Reject:
///             roles = (job.metadata or {}).get("roles", [])
///             return Pass() if self.role in roles else Reject(f"missing role: {self.role}")
///
///     register_action("has_role", HasRole)
///
/// The action is then available in YAML config under its registered name::
///
///     checks:
///       gate:
///         type: has_role
///         role: privileged
#[pyfunction]
fn register_action(py: Python<'_>, name: String, cls: Bound<'_, PyAny>) -> PyResult<()> {
    // 1. Detect kind and validate the required async method.
    let kind = detect_kind(py, &cls)?;
    let method_name = match kind {
        ActionKind::Check => "evaluate",
        ActionKind::Transform => "execute",
        ActionKind::Target => "dispatch",
    };
    require_async_method(py, &cls, method_name)?;

    // 2. Capture a GIL-independent reference to the class.
    let cls_ref: Py<PyAny> = cls.unbind();

    // 3. Build a RuntimeActionFactory that instantiates the class from YAML config.
    let factory: RuntimeActionFactory = Arc::new(move |config: serde_json::Value| {
        Python::attach(|py| -> Result<Action, ActionError> {
            // Config arrives as a serde_json::Value (object); convert to Python dict for **kwargs.
            let kwargs_obj: Py<PyAny> = pythonize::pythonize(py, &config)
                .map_err(|e| ActionError::ConfigError(format!("config pythonize: {}", e)))?
                .unbind();
            let kwargs = kwargs_obj.cast_bound::<PyDict>(py).map_err(|e| {
                ActionError::ConfigError(format!("config must be a dict, got: {}", e))
            })?;

            let instance = cls_ref
                .call(py, (), Some(kwargs))
                .map_err(|e| ActionError::ConfigError(format!("__init__ failed: {}", e)))?;

            Ok(match kind {
                ActionKind::Check => {
                    Action::Check(Arc::new(PyCheckAdapter { instance }), None, None)
                }
                ActionKind::Transform => {
                    Action::Transform(Arc::new(PyTransformAdapter { instance }), None, None)
                }
                ActionKind::Target => {
                    Action::Target(Arc::new(PyTargetAdapter { instance }), None, None)
                }
            })
        })
    });

    // 4. Register with the core bits runtime registry.
    register_runtime_action(&name, factory).map_err(|e| PyValueError::new_err(e.to_string()))
}

// ============================================================
//  Bits Python class
// ============================================================

#[pyclass(name = "Bits")]
struct BitsPy {
    inner: Arc<bits::Bits>,
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
            let bits = bits::Bits::from_config(&config).map_err(to_value_error)?;
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
            match inner.submit(Job::new(request_value)) {
                bits::SubmitOutcome::Accepted(handle) => Ok(handle.id),
                bits::SubmitOutcome::Overloaded => Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "broker at capacity",
                )),
            }
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
        PollOutcome::Pending { id, .. } => Python::attach(|py| {
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
        JobResult::Redirect {
            location,
            message,
            content_type,
            content_length,
        } => Python::attach(|py| {
            let payload = PyDict::new(py);
            payload.set_item("status", "redirect")?;
            payload.set_item("location", location)?;
            payload.set_item("message", message)?;
            if let Some(content_type) = content_type {
                payload.set_item("content_type", content_type)?;
            }
            if let Some(content_length) = content_length {
                payload.set_item("content_length", content_length)?;
            }
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
        JobResult::Overloaded { reason } => Python::attach(|py| {
            let payload = PyDict::new(py);
            payload.set_item("status", "overloaded")?;
            payload.set_item("reason", reason)?;
            Ok(payload.into_any().unbind())
        }),
        JobResult::RateLimited { reason } => Python::attach(|py| {
            let payload = PyDict::new(py);
            payload.set_item("status", "rate_limited")?;
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

// ============================================================
//  Module definition
// ============================================================

#[pymodule]
fn bits_py(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Bits broker
    m.add_class::<BitsPy>()?;

    // Action base classes (ABC-style)
    m.add_class::<PyCheckAction>()?;
    m.add_class::<PyTransformAction>()?;
    m.add_class::<PyTargetAction>()?;

    // Outcome types
    m.add_class::<PyPass>()?;
    m.add_class::<PyContinue>()?;
    m.add_class::<PyReject>()?;
    m.add_class::<PySuccess>()?;
    m.add_class::<PyRedirect>()?;
    m.add_class::<PyError>()?;

    // Job view
    m.add_class::<PyJob>()?;

    // Registration
    m.add_function(wrap_pyfunction!(register_action, m)?)?;

    Ok(())
}

using Microsoft.AspNetCore.Mvc;
using PayBridge.PaymentApi.Services;
using PayBridge.Shared.Models;

namespace PayBridge.PaymentApi.Controllers;

[ApiController]
[Route("api/payments")]
public class PaymentsController : ControllerBase
{
    private readonly PaymentService _paymentService;
    private readonly ILogger<PaymentsController> _logger;

    public PaymentsController(PaymentService paymentService, ILogger<PaymentsController> logger)
    {
        _paymentService = paymentService;
        _logger = logger;
    }

    /// <summary>Create a new payment. Idempotent via IdempotencyKey.</summary>
    [HttpPost]
    [ProducesResponseType(typeof(PaymentResponse), StatusCodes.Status201Created)]
    [ProducesResponseType(typeof(PaymentResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status503ServiceUnavailable)]
    public async Task<IActionResult> CreatePayment(
        [FromBody] CreatePaymentRequest request,
        CancellationToken ct)
    {
        if (!ModelState.IsValid) return BadRequest(ModelState);

        try
        {
            var (response, wasCached) = await _paymentService.CreateAsync(request, ct);
            return wasCached
                ? Ok(response)
                : CreatedAtAction(nameof(GetPayment), new { paymentId = response.PaymentId }, response);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("disabled"))
        {
            return StatusCode(StatusCodes.Status503ServiceUnavailable,
                new { error = "payment_processing_disabled", message = ex.Message });
        }
    }

    /// <summary>Get payment status by ID.</summary>
    [HttpGet("{paymentId:guid}")]
    [ProducesResponseType(typeof(PaymentResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetPayment(Guid paymentId, CancellationToken ct)
    {
        var payment = await _paymentService.GetAsync(paymentId, ct);
        if (payment is null) return NotFound();
        return Ok(payment);
    }
}

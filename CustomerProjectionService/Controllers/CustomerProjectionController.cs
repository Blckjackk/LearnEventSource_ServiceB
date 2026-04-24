using CustomerProjectionService.Data;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace CustomerProjectionService.Controllers;

[ApiController]
[Route("api/customer")]
public class CustomerProjectionController : ControllerBase
{
    private readonly AppDbContext _db;

    public CustomerProjectionController(AppDbContext db)
    {
        _db = db;
    }

    [HttpGet]
    public async Task<IActionResult> GetAll()
    {
        var data = await _db.Customers.AsNoTracking().Where(x => !x.IsDeleted).ToListAsync();
        return Ok(data);
    }

    [HttpGet("{id:guid}")]
    public async Task<IActionResult> GetById(Guid id)
    {
        var customer = await _db.Customers.AsNoTracking().FirstOrDefaultAsync(x => x.Id == id && !x.IsDeleted);
        if (customer is null)
        {
            return NotFound();
        }

        return Ok(customer);
    }
}

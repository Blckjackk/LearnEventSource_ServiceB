using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CustomerProjectionService.Migrations
{
    /// <inheritdoc />
    public partial class AddProjectionCheckpointAndCustomerDetailsv2 : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "ProjectionCheckpoints",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    CommitPosition = table.Column<decimal>(type: "decimal(20,0)", nullable: false),
                    PreparePosition = table.Column<decimal>(type: "decimal(20,0)", nullable: false),
                    UpdatedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ProjectionCheckpoints", x => x.Id);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "ProjectionCheckpoints");
        }
    }
}
